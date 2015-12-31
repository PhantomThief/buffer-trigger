/**
 * 
 */
package com.github.phantomthief.collection.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.github.phantomthief.collection.BufferTrigger;

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

    BatchConsumeBlockingQueueTrigger(boolean forceConsumeEveryTick, int batchConsumerSize,
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
                                    e.printStackTrace();
                                    ex.printStackTrace();
                                }
                            } else {
                                logger.error("Ops.", e);
                            }
                        }
                    }
                }
            }
        }, tickTime, tickTime, MILLISECONDS);
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
                                e.printStackTrace();
                                ex.printStackTrace();
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

    public static BatchConsumerTriggerBuilder<Object> newBuilder() {
        return new BatchConsumerTriggerBuilder<>();
    }
}
