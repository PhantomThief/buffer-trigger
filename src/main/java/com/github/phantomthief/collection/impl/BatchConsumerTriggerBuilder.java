/**
 * 
 */
package com.github.phantomthief.collection.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.util.ThrowableConsumer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

@SuppressWarnings({ "unchecked", "rawtypes" })
public final class BatchConsumerTriggerBuilder<E> {

    private static final long DEFAULT_TICK_TIME = SECONDS.toMillis(1);

    private ScheduledExecutorService scheduledExecutorService;
    private long consumePeriod;
    private int batchConsumerSize;
    private ThrowableConsumer<List<E>, Exception> consumer;
    private BiConsumer<Throwable, List<E>> exceptionHandler;

    public BatchConsumerTriggerBuilder<E>
            setScheduleExecutorService(ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
        return this;
    }

    @Deprecated
    public BatchConsumerTriggerBuilder<E> forceConsumeEveryTick() {
        return this;
    }

    /**
     * use {@link #consumePeriod(long, TimeUnit)} instead
     */
    @Deprecated
    public BatchConsumerTriggerBuilder<E> tickTime(long time, TimeUnit unit) {
        return consumePeriod(time, unit);
    }

    public BatchConsumerTriggerBuilder<E> consumePeriod(long time, TimeUnit unit) {
        this.consumePeriod = unit.toMillis(time);
        return this;
    }

    public BatchConsumerTriggerBuilder<E> batchConsumerSize(int size) {
        this.batchConsumerSize = size;
        return this;
    }

    @Deprecated
    public <E1> BatchConsumerTriggerBuilder<E1> setQueue(BlockingQueue<? extends E> queue) {
        return (BatchConsumerTriggerBuilder<E1>) this;
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

    @Deprecated
    public BatchConsumerTriggerBuilder<E> queueCapacity(int capacity) {
        return this;
    }

    public <E1> BufferTrigger<E1> build() {
        return (BufferTrigger<E1>) new LazyBufferTrigger<>(() -> {
            ensure();
            return new BatchConsumeBlockingQueueTrigger(consumePeriod, batchConsumerSize,
                    exceptionHandler, consumer, scheduledExecutorService);
        });
    }

    private void ensure() {
        checkNotNull(consumer);

        if (consumePeriod <= 0) {
            consumePeriod = DEFAULT_TICK_TIME;
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