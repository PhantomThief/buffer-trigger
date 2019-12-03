package com.github.phantomthief.collection.impl;

import static com.github.phantomthief.concurrent.MoreFutures.scheduleWithDynamicDelay;
import static com.github.phantomthief.util.MoreLocks.runWithLock;
import static com.github.phantomthief.util.MoreLocks.runWithTryLock;
import static java.lang.Integer.max;
import static java.lang.Math.min;
import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.util.ThrowableConsumer;

/**
 * @author w.vela
 */
public class BatchConsumeBlockingQueueTrigger<E> implements BufferTrigger<E> {

    private static final Logger logger = getLogger(BatchConsumeBlockingQueueTrigger.class);

    private final BlockingQueue<E> queue;
    private final IntSupplier batchSize;
    private final Supplier<Duration> linger;
    private final ThrowableConsumer<List<E>, Exception> consumer;
    private final BiConsumer<Throwable, List<E>> exceptionHandler;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicBoolean running = new AtomicBoolean();

    BatchConsumeBlockingQueueTrigger(BatchConsumerTriggerBuilder<E> builder) {
        this.linger = builder.linger;
        this.batchSize = builder.batchSize;
        this.queue = new LinkedBlockingQueue<>(max(builder.bufferSize, batchSize.getAsInt()));
        this.consumer = builder.consumer;
        this.exceptionHandler = builder.exceptionHandler;
        this.scheduledExecutorService = builder.scheduledExecutorService;
        scheduleWithDynamicDelay(scheduledExecutorService, linger, () -> doBatchConsumer(TriggerType.LINGER));
    }

    /**
     * use {@link com.github.phantomthief.collection.BufferTrigger#batchBlocking} instead
     * or {@link com.github.phantomthief.collection.BufferTrigger#batchBlockingTrigger}
     */
    @Deprecated
    public static BatchConsumerTriggerBuilder<Object> newBuilder() {
        return new BatchConsumerTriggerBuilder<>();
    }

    @Override
    public void enqueue(E element) {
        try {
            queue.put(element);
            tryTrigBatchConsume();
        } catch (InterruptedException e) {
            currentThread().interrupt();
        }
    }

    private void tryTrigBatchConsume() {
        int thisBatchSize = batchSize.getAsInt();
        if (queue.size() >= thisBatchSize) {
            runWithTryLock(lock, () -> {
                if (queue.size() >= thisBatchSize) {
                    if (!running.get()) { // prevent repeat enqueue
                        this.scheduledExecutorService.execute(() -> doBatchConsumer(TriggerType.ENQUEUE));
                        running.set(true);
                    }
                }
            });
        }
    }

    @Override
    public void manuallyDoTrigger() {
        doBatchConsumer(TriggerType.MANUALLY);
    }

    private void doBatchConsumer(TriggerType type) {
        runWithLock(lock, () -> {
            try {
                running.set(true);
                int queueSizeBeforeConsumer = queue.size();
                int consumedSize = 0;
                int thisBatchSize = batchSize.getAsInt();
                while (!queue.isEmpty()) {
                    if (queue.size() < thisBatchSize) {
                        if (type == TriggerType.ENQUEUE) {
                            return;
                        } else if (type == TriggerType.LINGER
                                && consumedSize >= queueSizeBeforeConsumer) {
                            return;
                        }
                    }
                    List<E> toConsumeData = new ArrayList<>(min(thisBatchSize, queue.size()));
                    queue.drainTo(toConsumeData, thisBatchSize);
                    if (!toConsumeData.isEmpty()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("do batch consumer:{}, size:{}", type,
                                    toConsumeData.size());
                        }
                        consumedSize += toConsumeData.size();
                        doConsume(toConsumeData);
                    }
                }
            } finally {
                running.set(false);
            }
        });
    }

    private void doConsume(List<E> toConsumeData) {
        try {
            consumer.accept(toConsumeData);
        } catch (Throwable e) {
            if (exceptionHandler != null) {
                try {
                    exceptionHandler.accept(e, toConsumeData);
                } catch (Throwable ex) {
                    logger.error("", ex);
                }
            } else {
                logger.error("Ops.", e);
            }
        }
    }

    @Override
    public long getPendingChanges() {
        return queue.size();
    }

    /**
     * just for log and debug
     */
    private enum TriggerType {
        LINGER, ENQUEUE, MANUALLY
    }
}
