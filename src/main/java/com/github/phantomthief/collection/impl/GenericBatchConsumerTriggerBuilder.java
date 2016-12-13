/**
 * 
 */
package com.github.phantomthief.collection.impl;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.util.ThrowableConsumer;

/**
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

    public GenericBatchConsumerTriggerBuilder<E>
            setScheduleExecutorService(ScheduledExecutorService scheduledExecutorService) {
        builder.setScheduleExecutorService(scheduledExecutorService);
        return this;
    }

    /**
     * use {@link #linger(long, TimeUnit)} instead
     */
    @Deprecated
    public GenericBatchConsumerTriggerBuilder<E> tickTime(long time, TimeUnit unit) {
        builder.tickTime(time, unit);
        return this;
    }

    public GenericBatchConsumerTriggerBuilder<E> linger(long time, TimeUnit unit) {
        builder.linger(time, unit);
        return this;
    }

    public GenericBatchConsumerTriggerBuilder<E> linger(Duration duration) {
        builder.linger(duration);
        return this;
    }

    /**
     * use {@link #batchSize} instead
     */
    @Deprecated
    public GenericBatchConsumerTriggerBuilder<E> batchConsumerSize(int size) {
        builder.batchConsumerSize(size);
        return this;
    }

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
     * use {@link #setConsumerEx}
     */
    @Deprecated
    public GenericBatchConsumerTriggerBuilder<E> setConsumer(Consumer<? super List<E>> consumer) {
        builder.setConsumer(consumer);
        return this;
    }

    public GenericBatchConsumerTriggerBuilder<E>
            setConsumerEx(ThrowableConsumer<? super List<E>, Exception> consumer) {
        builder.setConsumerEx(consumer);
        return this;
    }

    public GenericBatchConsumerTriggerBuilder<E>
            setExceptionHandler(BiConsumer<? super Throwable, ? super List<E>> exceptionHandler) {
        builder.setExceptionHandler(exceptionHandler);
        return this;
    }

    @Deprecated
    public GenericBatchConsumerTriggerBuilder<E> queueCapacity(int capacity) {
        builder.queueCapacity(capacity);
        return this;
    }

    public BufferTrigger<E> build() {
        return builder.build();
    }
}
