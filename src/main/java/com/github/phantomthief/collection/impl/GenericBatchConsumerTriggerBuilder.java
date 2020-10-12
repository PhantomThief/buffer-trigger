package com.github.phantomthief.collection.impl;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.util.ThrowableConsumer;

/**
 * {@link BatchConsumeBlockingQueueTrigger}通用构造器.
 * <p>
 * 当对{@link BatchConsumeBlockingQueueTrigger}有更多自定义配置时可使用该构造器.
 * <p>
 * 本质上包装了{@link BatchConsumerTriggerBuilder}，屏蔽底层细节.
 * @author w.vela
 */
public class GenericBatchConsumerTriggerBuilder<E> {

    private final BatchConsumerTriggerBuilder<Object> builder;

    public GenericBatchConsumerTriggerBuilder(BatchConsumerTriggerBuilder<Object> builder) {
        this.builder = builder;
    }

    @Deprecated
    public GenericBatchConsumerTriggerBuilder<E> forceConsumeEveryTick() {
        builder.forceConsumeEveryTick();
        return this;
    }

    /**
     * 自定义计划任务线程池，推荐使用内部默认实现.
     * <p>
     * 如使用自定义线程池，在调用{@link BufferTrigger#close()}方法后需要手动调用{@link ScheduledExecutorService#shutdown()}
     * 以及{@link ScheduledExecutorService#awaitTermination(long, TimeUnit)}方法来保证线程池平滑停止.
     */
    public GenericBatchConsumerTriggerBuilder<E>
            setScheduleExecutorService(ScheduledExecutorService scheduledExecutorService) {
        builder.setScheduleExecutorService(scheduledExecutorService);
        return this;
    }

    /**
     * 该方法已废弃，请使用{@link #linger(long, TimeUnit)}替代.
     */
    @Deprecated
    public GenericBatchConsumerTriggerBuilder<E> tickTime(long time, TimeUnit unit) {
        builder.tickTime(time, unit);
        return this;
    }

    /**
     * 设置消息批处理延迟等待时间，与{@link #batchSize(int)}结合，类似TCP Nagle.
     */
    public GenericBatchConsumerTriggerBuilder<E> linger(long time, TimeUnit unit) {
        builder.linger(time, unit);
        return this;
    }

    /**
     * {@link #linger(long, TimeUnit)}支持{@link Duration}版本.
     */
    public GenericBatchConsumerTriggerBuilder<E> linger(Duration duration) {
        builder.linger(duration);
        return this;
    }

    /**
     * {@link #linger(long, TimeUnit)}支持{@link Duration}提供器版本，用于动态配置场景.
     */
    public GenericBatchConsumerTriggerBuilder<E> linger(Supplier<Duration> duration) {
        builder.linger(duration);
        return this;
    }

    /**
     * 该方法已废弃，请使用{@link #batchSize}替代.
     */
    @Deprecated
    public GenericBatchConsumerTriggerBuilder<E> batchConsumerSize(int size) {
        builder.batchConsumerSize(size);
        return this;
    }

    /**
     * 设置进行批处理的元素数量阈值
     */
    public GenericBatchConsumerTriggerBuilder<E> batchSize(int size) {
        builder.batchSize(size);
        return this;
    }

    @Deprecated
    public GenericBatchConsumerTriggerBuilder<E> setQueue(BlockingQueue<? extends E> queue) {
        builder.setQueue(queue);
        return this;
    }

    /**
     * 该方法已废弃，请使用{@link #setConsumerEx}替代.
     */
    @Deprecated
    public GenericBatchConsumerTriggerBuilder<E> setConsumer(Consumer<? super List<E>> consumer) {
        builder.setConsumer(consumer);
        return this;
    }

    /**
     * 设置消费回调函数.
     * <p>
     * 该方法用于设定消费行为，由使用者自行提供消费回调函数，需要注意，
     * 回调注入的对象为缓存队列中尚存的所有元素，非逐个元素消费.
     */
    public GenericBatchConsumerTriggerBuilder<E>
            setConsumerEx(ThrowableConsumer<? super List<E>, Exception> consumer) {
        builder.setConsumerEx(consumer);
        return this;
    }

    /**
     * 设置异常处理器.
     * <p>
     * 该处理器会在消费异常时执行.
     */
    public GenericBatchConsumerTriggerBuilder<E>
            setExceptionHandler(BiConsumer<? super Throwable, ? super List<E>> exceptionHandler) {
        builder.setExceptionHandler(exceptionHandler);
        return this;
    }

    /**
     * 该方法已废弃，请使用{@link #bufferSize}替代.
     */
    @Deprecated
    public GenericBatchConsumerTriggerBuilder<E> queueCapacity(int capacity) {
        builder.queueCapacity(capacity);
        return this;
    }

    /**
     * 设置缓存队列最大容量.
     */
    public GenericBatchConsumerTriggerBuilder<E> bufferSize(int bufferSize) {
        builder.bufferSize(bufferSize);
        return this;
    }

    /**
     * 生成实例.
     */
    public BufferTrigger<E> build() {
        return builder.build();
    }
}
