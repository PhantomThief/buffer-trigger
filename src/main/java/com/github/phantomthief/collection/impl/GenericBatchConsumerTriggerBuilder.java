/**
 * 
 */
package com.github.phantomthief.collection.impl;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.github.phantomthief.collection.BufferTrigger;

/**
 * @author w.vela
 */
public class GenericBatchConsumerTriggerBuilder<E> {

    private final BatchConsumerTriggerBuilder<Object> builder;

    GenericBatchConsumerTriggerBuilder(BatchConsumerTriggerBuilder<Object> builder) {
        this.builder = builder;
    }

    public GenericBatchConsumerTriggerBuilder<E> forceConsumeEveryTick() {
        builder.forceConsumeEveryTick();
        return this;
    }

    public GenericBatchConsumerTriggerBuilder<E> setScheduleExecutorService(
            ScheduledExecutorService scheduledExecutorService) {
        builder.setScheduleExecutorService(scheduledExecutorService);
        return this;
    }

    public GenericBatchConsumerTriggerBuilder<E> tickTime(long time, TimeUnit unit) {
        builder.tickTime(time, unit);
        return this;
    }

    public GenericBatchConsumerTriggerBuilder<E> batchConsumerSize(int size) {
        builder.batchConsumerSize(size);
        return this;
    }

    public GenericBatchConsumerTriggerBuilder<E> setQueue(BlockingQueue<? extends E> queue) {
        builder.setQueue(queue);
        return this;
    }

    public GenericBatchConsumerTriggerBuilder<E> setConsumer(Consumer<? super List<E>> consumer) {
        builder.setConsumer(consumer);
        return this;
    }

    public GenericBatchConsumerTriggerBuilder<E> setExceptionHandler(
            BiConsumer<? super Throwable, ? super List<E>> exceptionHandler) {
        builder.setExceptionHandler(exceptionHandler);
        return this;
    }

    public GenericBatchConsumerTriggerBuilder<E> queueCapacity(int capacity) {
        builder.queueCapacity(capacity);
        return this;
    }

    public BufferTrigger<E> build() {
        return builder.build();
    }
}
