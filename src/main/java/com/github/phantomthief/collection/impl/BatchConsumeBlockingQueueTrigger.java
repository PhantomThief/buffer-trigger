package com.github.phantomthief.collection.impl;

import static com.github.phantomthief.util.MoreLocks.runWithLock;
import static com.github.phantomthief.util.MoreLocks.runWithTryLock;
import static java.lang.Integer.max;
import static java.lang.Math.min;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

import org.slf4j.Logger;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.util.ThrowableConsumer;

/**
 * @author w.vela
 */
public class BatchConsumeBlockingQueueTrigger<E> implements BufferTrigger<E> {

    private static final Logger logger = getLogger(BatchConsumeBlockingQueueTrigger.class);

    private final BlockingQueue<E> queue;
    private final int batchSize;
    private final long lingerMs;
    private final ThrowableConsumer<List<E>, Exception> consumer;
    private final BiConsumer<Throwable, List<E>> exceptionHandler;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicBoolean running = new AtomicBoolean();

    BatchConsumeBlockingQueueTrigger(long lingerMs, int batchSize, int bufferSize,
            BiConsumer<Throwable, List<E>> exceptionHandler,
            ThrowableConsumer<List<E>, Exception> consumer,
            ScheduledExecutorService scheduledExecutorService) {
        this.lingerMs = lingerMs;
        this.batchSize = batchSize;
        this.queue = new LinkedBlockingQueue<>(max(bufferSize, batchSize));
        this.consumer = consumer;
        this.exceptionHandler = exceptionHandler;
        this.scheduledExecutorService = scheduledExecutorService;
        this.scheduledExecutorService.schedule(new BatchConsumerRunnable(), this.lingerMs,
                MILLISECONDS);
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
        if (queue.size() >= batchSize) {
            runWithTryLock(lock, () -> {
                if (queue.size() >= batchSize) {
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
                while (!queue.isEmpty()) {
                    if (queue.size() < batchSize) {
                        if (type == TriggerType.ENQUEUE) {
                            return;
                        } else if (type == TriggerType.LINGER
                                && consumedSize >= queueSizeBeforeConsumer) {
                            return;
                        }
                    }
                    List<E> toConsumeData = new ArrayList<>(min(batchSize, queue.size()));
                    queue.drainTo(toConsumeData, batchSize);
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

    private class BatchConsumerRunnable implements Runnable {

        @Override
        public void run() {
            try {
                doBatchConsumer(TriggerType.LINGER);
            } finally {
                scheduledExecutorService.schedule(this, lingerMs, MILLISECONDS);
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
