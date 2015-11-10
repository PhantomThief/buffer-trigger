/**
 * 
 */
package com.github.phantomthief.collection.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.github.phantomthief.collection.BufferTrigger;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author w.vela
 */
public class BatchConsumeBlockingQueueTrigger<E> implements BufferTrigger<E> {

    private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());

    private final int batchConsumerSize;
    private final BlockingQueue<E> queue;
    private final Consumer<List<E>> consumer;
    private final BiConsumer<Throwable, List<E>> exceptionHandler;
    private final ScheduledExecutorService scheduledExecutorService;

    private BatchConsumeBlockingQueueTrigger(boolean forceConsumeEveryTick, int batchConsumerSize,
            BlockingQueue<E> queue, BiConsumer<Throwable, List<E>> exceptionHandler,
            Consumer<List<E>> consumer, ScheduledExecutorService scheduledExecutorService,
            long tickTime) {
        this.batchConsumerSize = batchConsumerSize;
        this.queue = queue;
        this.consumer = consumer;
        this.exceptionHandler = exceptionHandler;
        this.scheduledExecutorService = scheduledExecutorService;
        this.scheduledExecutorService.scheduleWithFixedDelay(() -> {
            synchronized (BatchConsumeBlockingQueueTrigger.this) {
                while (queue.size() >= batchConsumerSize
                        || (forceConsumeEveryTick && !queue.isEmpty())) {
                    List<E> toConsumerData = new ArrayList<>(
                            Math.min(batchConsumerSize, queue.size()));
                    queue.drainTo(toConsumerData, batchConsumerSize);
                    if (!toConsumerData.isEmpty()) {
                        try {
                            consumer.accept(toConsumerData);
                        } catch (Throwable e) {
                            if (exceptionHandler != null) {
                                try {
                                    exceptionHandler.accept(e, toConsumerData);
                                } catch (Throwable ex) {
                                    // do nothing;
                                }
                            } else {
                                logger.error("Ops.", e);
                            }
                        }
                    }
                }
            }
        } , tickTime, tickTime, MILLISECONDS);
    }

    @Override
    public void enqueue(E element) {
        try {
            queue.put(element);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /* (non-Javadoc)
     * @see com.github.phantomthief.collection.BufferTrigger#manuallyDoTrigger()
     */
    @Override
    public void manuallyDoTrigger() {
        synchronized (BatchConsumeBlockingQueueTrigger.this) {
            while (!queue.isEmpty()) {
                List<E> toConsumerData = new ArrayList<>(Math.min(queue.size(), batchConsumerSize));
                queue.drainTo(toConsumerData, batchConsumerSize);
                if (!toConsumerData.isEmpty()) {
                    try {
                        consumer.accept(toConsumerData);
                    } catch (Throwable e) {
                        if (exceptionHandler != null) {
                            try {
                                exceptionHandler.accept(e, toConsumerData);
                            } catch (Throwable ex) {
                                // do nothing;
                            }
                        } else {
                            logger.error("Ops.", e);
                        }
                    }
                }
            }
        }
    }

    /* (non-Javadoc)
     * @see com.github.phantomthief.collection.BufferTrigger#getPendingChanges()
     */
    @Override
    public long getPendingChanges() {
        return queue.size();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static final class Builder<E> {

        private static final int ARRAY_LIST_THRESHOLD = 1000;
        private static final long DEFAULT_TICK_TIME = SECONDS.toMillis(1);

        private ScheduledExecutorService scheduledExecutorService;
        private long tickTime;
        private boolean forceConsumeEveryTick;
        private int batchConsumerSize;
        private BlockingQueue<E> queue;
        private Consumer<List<E>> consumer;
        private BiConsumer<Throwable, List<E>> exceptionHandler;

        public Builder<E> forceConsumeEveryTick() {
            this.forceConsumeEveryTick = true;
            return this;
        }

        public Builder<E>
                setScheduleExecutorService(ScheduledExecutorService scheduledExecutorService) {
            this.scheduledExecutorService = scheduledExecutorService;
            return this;
        }

        public Builder<E> tickTime(long time, TimeUnit unit) {
            this.tickTime = unit.toMillis(time);
            return this;
        }

        public Builder<E> batchConsumerSize(int size) {
            this.batchConsumerSize = size;
            return this;
        }

        public <E1> Builder<E1> setQueue(BlockingQueue<? extends E> queue) {
            Builder<E1> thisBuilder = (Builder<E1>) this;
            thisBuilder.queue = (BlockingQueue<E1>) queue;
            return thisBuilder;
        }

        public <E1> Builder<E1> setConsumer(Consumer<? super List<E1>> consumer) {
            Builder<E1> thisBuilder = (Builder<E1>) this;
            thisBuilder.consumer = (Consumer) consumer;
            return thisBuilder;
        }

        public <E1> Builder<E1> setExceptionHandler(
                BiConsumer<? super Throwable, ? super List<E1>> exceptionHandler) {
            Builder<E1> thisBuilder = (Builder<E1>) this;
            thisBuilder.exceptionHandler = (BiConsumer) exceptionHandler;
            return thisBuilder;
        }

        public Builder<E> queueCapacity(int capacity) {
            if (capacity > ARRAY_LIST_THRESHOLD) {
                this.queue = new LinkedBlockingDeque<>(capacity);
            } else {
                this.queue = new ArrayBlockingQueue<>(capacity);
            }
            return this;
        }

        public <E1> BufferTrigger<E1> build() {
            ensure();
            return new BatchConsumeBlockingQueueTrigger(forceConsumeEveryTick, batchConsumerSize,
                    queue, exceptionHandler, consumer, scheduledExecutorService, tickTime);
        }

        private void ensure() {
            checkNotNull(consumer);

            if (tickTime <= 0) {
                tickTime = DEFAULT_TICK_TIME;
            }
            if (queue == null) {
                queue = new LinkedBlockingQueue<>();
            }
            if (scheduledExecutorService == null) {
                scheduledExecutorService = makeScheduleExecutor();
            }
        }

        private ScheduledExecutorService makeScheduleExecutor() {
            ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1,
                    new ThreadFactoryBuilder()
                            .setNameFormat("pool-batch-consume-blocking-queue-thread-%d").build());

            return scheduledExecutorService;
        }
    }

    public static Builder<Object> newBuilder() {
        return new Builder<>();
    }
}
