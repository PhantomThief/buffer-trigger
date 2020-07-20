package com.github.phantomthief.collection.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.time.Duration.ofNanos;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.Executors.newScheduledThreadPool;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.util.ThrowableConsumer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

@SuppressWarnings({ "unchecked", "rawtypes" })
public final class BatchConsumerTriggerBuilder<E> {

    private static final Duration DEFAULT_LINGER = ofSeconds(1);

    ScheduledExecutorService scheduledExecutorService;
    boolean usingInnerExecutor;
    Supplier<Duration> linger;
    int batchSize;
    int bufferSize;
    ThrowableConsumer<List<E>, Exception> consumer;
    BiConsumer<Throwable, List<E>> exceptionHandler;

    /**
     * If you create own ScheduledExecutorService, then you have to shutdown it yourself.
     */
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
     * use {@link #linger(long, TimeUnit)} instead
     */
    @Deprecated
    public BatchConsumerTriggerBuilder<E> tickTime(long time, TimeUnit unit) {
        return linger(time, unit);
    }

    public BatchConsumerTriggerBuilder<E> linger(long time, TimeUnit unit) {
        return linger(() -> ofNanos(unit.toNanos(time)));
    }

    public BatchConsumerTriggerBuilder<E> linger(@Nonnull Duration duration) {
        checkNotNull(duration);
        return linger(() -> duration);
    }

    public BatchConsumerTriggerBuilder<E> linger(@Nonnull Supplier<Duration> duration) {
        this.linger = checkNotNull(duration);
        return this;
    }

    /**
     * use {@link #batchSize} instead
     */
    @Deprecated
    public BatchConsumerTriggerBuilder<E> batchConsumerSize(int size) {
        return batchSize(size);
    }

    public BatchConsumerTriggerBuilder<E> batchSize(int size) {
        this.batchSize = size;
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

    /**
     * use {@link #bufferSize} instead
     */
    @Deprecated
    public BatchConsumerTriggerBuilder<E> queueCapacity(int capacity) {
        return bufferSize(capacity);
    }

    public BatchConsumerTriggerBuilder<E> bufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    public <E1> BufferTrigger<E1> build() {
        return (BufferTrigger<E1>) new LazyBufferTrigger<>(() -> {
            ensure();
            return new BatchConsumeBlockingQueueTrigger(this);
        });
    }

    private void ensure() {
        checkNotNull(consumer);

        if (linger == null) {
            linger = () -> DEFAULT_LINGER;
        }
        if (scheduledExecutorService == null) {
            scheduledExecutorService = makeScheduleExecutor();
            usingInnerExecutor = true;
        }
    }

    private ScheduledExecutorService makeScheduleExecutor() {

        return newScheduledThreadPool(1,
                new ThreadFactoryBuilder()
                        .setNameFormat("pool-batch-consume-blocking-queue-thread-%d")
                        .setDaemon(true).build());
    }
}