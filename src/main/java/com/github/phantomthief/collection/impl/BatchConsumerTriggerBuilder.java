/**
 * 
 */
package com.github.phantomthief.collection.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.util.ThrowableConsumer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

@SuppressWarnings({ "unchecked", "rawtypes" })
public final class BatchConsumerTriggerBuilder<E> {

    private static final int ARRAY_LIST_THRESHOLD = 1000;
    private static final long DEFAULT_TICK_TIME = SECONDS.toMillis(1);

    private ScheduledExecutorService scheduledExecutorService;
    private long tickTime;
    private boolean forceConsumeEveryTick;
    private int batchConsumerSize;
    private BlockingQueue<E> queue;
    private ThrowableConsumer<List<E>, Exception> consumer;
    private BiConsumer<Throwable, List<E>> exceptionHandler;

    public BatchConsumerTriggerBuilder<E> forceConsumeEveryTick() {
        this.forceConsumeEveryTick = true;
        return this;
    }

    public BatchConsumerTriggerBuilder<E>
            setScheduleExecutorService(ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
        return this;
    }

    public BatchConsumerTriggerBuilder<E> tickTime(long time, TimeUnit unit) {
        this.tickTime = unit.toMillis(time);
        return this;
    }

    public BatchConsumerTriggerBuilder<E> batchConsumerSize(int size) {
        this.batchConsumerSize = size;
        return this;
    }

    public <E1> BatchConsumerTriggerBuilder<E1> setQueue(BlockingQueue<? extends E> queue) {
        BatchConsumerTriggerBuilder<E1> thisBuilder = (BatchConsumerTriggerBuilder<E1>) this;
        thisBuilder.queue = (BlockingQueue<E1>) queue;
        return thisBuilder;
    }

    /**
     * use {@link #setConsumerEx}
     */
    @Deprecated
    public <E1> BatchConsumerTriggerBuilder<E1> setConsumer(Consumer<? super List<E1>> consumer) {
        BatchConsumerTriggerBuilder<E1> thisBuilder = (BatchConsumerTriggerBuilder<E1>) this;
        thisBuilder.consumer = consumer::accept;
        return thisBuilder;
    }

    public <E1> BatchConsumerTriggerBuilder<E1>
            setConsumerEx(ThrowableConsumer<? super List<E1>, Exception> consumer) {
        BatchConsumerTriggerBuilder<E1> thisBuilder = (BatchConsumerTriggerBuilder<E1>) this;
        thisBuilder.consumer = consumer::accept;
        return thisBuilder;
    }

    public <E1> BatchConsumerTriggerBuilder<E1>
            setExceptionHandler(BiConsumer<? super Throwable, ? super List<E1>> exceptionHandler) {
        BatchConsumerTriggerBuilder<E1> thisBuilder = (BatchConsumerTriggerBuilder<E1>) this;
        thisBuilder.exceptionHandler = (BiConsumer) exceptionHandler;
        return thisBuilder;
    }

    public BatchConsumerTriggerBuilder<E> queueCapacity(int capacity) {
        if (capacity > ARRAY_LIST_THRESHOLD) {
            this.queue = new LinkedBlockingDeque<>(capacity);
        } else {
            this.queue = new ArrayBlockingQueue<>(capacity);
        }
        return this;
    }

    public <E1> BufferTrigger<E1> build() {
        return (BufferTrigger<E1>) new LazyBufferTrigger<>(() -> {
            ensure();
            return new BatchConsumeBlockingQueueTrigger(forceConsumeEveryTick, batchConsumerSize,
                    queue, exceptionHandler, consumer, scheduledExecutorService, tickTime);
        });
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

        return newScheduledThreadPool(1,
                new ThreadFactoryBuilder()
                        .setNameFormat("pool-batch-consume-blocking-queue-thread-%d")
                        .setDaemon(true).build());
    }
}