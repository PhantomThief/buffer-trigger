/**
 * 
 */
package com.github.phantomthief.collection.impl;

import static java.lang.Math.min;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
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
    private final int batchConsumerSize;
    private final long consumePeriod;
    private final ThrowableConsumer<List<E>, Exception> consumer;
    private final BiConsumer<Throwable, List<E>> exceptionHandler;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Object lock = new Object();

    BatchConsumeBlockingQueueTrigger(long consumePeriod, int batchConsumerSize,
            BiConsumer<Throwable, List<E>> exceptionHandler,
            ThrowableConsumer<List<E>, Exception> consumer,
            ScheduledExecutorService scheduledExecutorService) {
        this.consumePeriod = consumePeriod;
        this.batchConsumerSize = batchConsumerSize;
        this.queue = new LinkedBlockingQueue<>(batchConsumerSize);
        this.consumer = consumer;
        this.exceptionHandler = exceptionHandler;
        this.scheduledExecutorService = scheduledExecutorService;
        this.scheduledExecutorService.schedule(new BatchConsumerRunnable(), this.consumePeriod,
                MILLISECONDS);
    }

    /**
     * use {@link com.github.phantomthief.collection.BufferTrigger#batchBlocking} instead
     */
    @Deprecated
    public static BatchConsumerTriggerBuilder<Object> newBuilder() {
        return new BatchConsumerTriggerBuilder<>();
    }

    @Override
    public void enqueue(E element) {
        try {
            queue.put(element);
            if (queue.size() >= batchConsumerSize) {
                synchronized (lock) {
                    if (queue.size() >= batchConsumerSize) {
                        this.scheduledExecutorService.execute(this::doBatchConsumer);
                    }
                }
            }
        } catch (InterruptedException e) {
            currentThread().interrupt();
        }
    }

    @Override
    public void manuallyDoTrigger() {
        doBatchConsumer();
    }

    private void doBatchConsumer() {
        synchronized (lock) {
            while (!queue.isEmpty()) {
                List<E> toConsumeData = new ArrayList<>(min(batchConsumerSize, queue.size()));
                queue.drainTo(toConsumeData, batchConsumerSize);
                if (!toConsumeData.isEmpty()) {
                    try {
                        consumer.accept(toConsumeData);
                    } catch (Throwable e) {
                        if (exceptionHandler != null) {
                            try {
                                exceptionHandler.accept(e, toConsumeData);
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

    private class BatchConsumerRunnable implements Runnable {

        @Override
        public void run() {
            try {
                doBatchConsumer();
            } finally {
                scheduledExecutorService.schedule(this, consumePeriod, MILLISECONDS);
            }
        }
    }

    @Override
    public long getPendingChanges() {
        return queue.size();
    }
}
