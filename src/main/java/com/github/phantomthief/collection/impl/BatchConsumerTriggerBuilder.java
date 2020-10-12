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

/**
 * {@link BatchConsumeBlockingQueueTrigger}构造器，目前已不推荐直接使用，
 * 请调用{@link BufferTrigger#batchBlocking()}生成构造器实例.
 * <p>
 * 用于标准化{@link BatchConsumeBlockingQueueTrigger}实例生成及配置，提供部分机制的默认实现.
 * @param <E>
 */
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
     * 自定义计划任务线程池，推荐使用内部默认实现.
     * <p>
     * 如使用自定义线程池，在调用{@link BufferTrigger#close()}方法后需要手动调用{@link ScheduledExecutorService#shutdown()}
     * 以及{@link ScheduledExecutorService#awaitTermination(long, TimeUnit)}方法来保证线程池平滑停止.
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
     * 该方法已废弃，请使用{@link #linger(long, TimeUnit)}替代.
     */
    @Deprecated
    public BatchConsumerTriggerBuilder<E> tickTime(long time, TimeUnit unit) {
        return linger(time, unit);
    }

    /**
     * 设置消息批处理延迟等待时间，与{@link #batchSize(int)}结合，类似TCP Nagle.
     */
    public BatchConsumerTriggerBuilder<E> linger(long time, TimeUnit unit) {
        return linger(() -> ofNanos(unit.toNanos(time)));
    }

    /**
     * {@link #linger(long, TimeUnit)}支持{@link Duration}版本.
     */
    public BatchConsumerTriggerBuilder<E> linger(@Nonnull Duration duration) {
        checkNotNull(duration);
        return linger(() -> duration);
    }

    /**
     * {@link #linger(long, TimeUnit)}支持{@link Duration}提供器版本，用于动态配置场景.
     */
    public BatchConsumerTriggerBuilder<E> linger(@Nonnull Supplier<Duration> duration) {
        this.linger = checkNotNull(duration);
        return this;
    }

    /**
     * 该方法已废弃，请使用{@link #batchSize}替代.
     */
    @Deprecated
    public BatchConsumerTriggerBuilder<E> batchConsumerSize(int size) {
        return batchSize(size);
    }

    /**
     * 设置进行批处理的元素数量阈值
     */
    public BatchConsumerTriggerBuilder<E> batchSize(int size) {
        this.batchSize = size;
        return this;
    }

    @Deprecated
    public <E1> BatchConsumerTriggerBuilder<E1> setQueue(BlockingQueue<? extends E> queue) {
        return (BatchConsumerTriggerBuilder<E1>) this;
    }

    /**
     * 该方法已废弃，请使用{@link #setConsumerEx}替代.
     */
    @Deprecated
    public <E1> BatchConsumerTriggerBuilder<E1> setConsumer(Consumer<? super List<E1>> consumer) {
        BatchConsumerTriggerBuilder<E1> thisBuilder = (BatchConsumerTriggerBuilder<E1>) this;
        thisBuilder.consumer = consumer::accept;
        return thisBuilder;
    }

    /**
     * 设置消费回调函数.
     * <p>
     * 该方法用于设定消费行为，由使用者自行提供消费回调函数，需要注意，
     * 回调注入的对象为缓存队列中尚存的所有元素，非逐个元素消费.
     */
    public <E1> BatchConsumerTriggerBuilder<E1>
            setConsumerEx(ThrowableConsumer<? super List<E1>, Exception> consumer) {
        BatchConsumerTriggerBuilder<E1> thisBuilder = (BatchConsumerTriggerBuilder<E1>) this;
        thisBuilder.consumer = consumer::accept;
        return thisBuilder;
    }

    /**
     * 设置异常处理器.
     * <p>
     * 该处理器会在消费异常时执行.
     */
    public <E1> BatchConsumerTriggerBuilder<E1>
            setExceptionHandler(BiConsumer<? super Throwable, ? super List<E1>> exceptionHandler) {
        BatchConsumerTriggerBuilder<E1> thisBuilder = (BatchConsumerTriggerBuilder<E1>) this;
        thisBuilder.exceptionHandler = (BiConsumer) exceptionHandler;
        return thisBuilder;
    }

    /**
     * 该方法已废弃，请使用{@link #bufferSize}替代.
     */
    @Deprecated
    public BatchConsumerTriggerBuilder<E> queueCapacity(int capacity) {
        return bufferSize(capacity);
    }

    /**
     * 设置缓存队列最大容量.
     */
    public BatchConsumerTriggerBuilder<E> bufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    /**
     * 生成实例.
     */
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