/**
 * 
 */
package com.github.phantomthief.collection.impl;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToIntBiFunction;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.collection.impl.SimpleBufferTrigger.TriggerStrategy;
import com.github.phantomthief.util.ThrowableConsumer;

/**
 * @author w.vela
 */
public class GenericSimpleBufferTriggerBuilder<E, C> {

    private final SimpleBufferTriggerBuilder<Object, Object> builder;

    GenericSimpleBufferTriggerBuilder(SimpleBufferTriggerBuilder<Object, Object> builder) {
        this.builder = builder;
    }

    /**
     * better use {@link #setContainerEx}
     *
     * @param queueAdder return if there is a change occurred.
     */
    public GenericSimpleBufferTriggerBuilder<E, C> setContainer(Supplier<? extends C> factory,
            BiPredicate<? super C, ? super E> queueAdder) {
        builder.setContainer(factory, queueAdder);
        return this;
    }

    /**
     * @param queueAdder return the change size occurred.
     */
    public GenericSimpleBufferTriggerBuilder<E, C> setContainerEx(Supplier<? extends C> factory,
            ToIntBiFunction<? super C, ? super E> queueAdder) {
        builder.setContainerEx(factory, queueAdder);
        return this;
    }

    public GenericSimpleBufferTriggerBuilder<E, C>
            setScheduleExecutorService(ScheduledExecutorService scheduledExecutorService) {
        builder.setScheduleExecutorService(scheduledExecutorService);
        return this;
    }

    public GenericSimpleBufferTriggerBuilder<E, C>
            setExceptionHandler(BiConsumer<? super Throwable, ? super C> exceptionHandler) {
        builder.setExceptionHandler(exceptionHandler);
        return this;
    }

    public GenericSimpleBufferTriggerBuilder<E, C>
            triggerStrategy(TriggerStrategy triggerStrategy) {
        builder.triggerStrategy(triggerStrategy);
        return this;
    }

    /**
     * use {@link #interval(long, TimeUnit)} or {@link #triggerStrategy}
     */
    @Deprecated
    public GenericSimpleBufferTriggerBuilder<E, C> on(long interval, TimeUnit unit, long count) {
        builder.on(interval, unit, count);
        return this;
    }

    public GenericSimpleBufferTriggerBuilder<E, C> interval(long interval, TimeUnit unit) {
        builder.interval(interval, unit);
        return this;
    }

    public GenericSimpleBufferTriggerBuilder<E, C> interval(LongSupplier interval, TimeUnit unit) {
        builder.interval(interval, unit);
        return this;
    }

    public GenericSimpleBufferTriggerBuilder<E, C>
            consumer(ThrowableConsumer<? super C, Throwable> consumer) {
        builder.consumer(consumer);
        return this;
    }

    public GenericSimpleBufferTriggerBuilder<E, C> maxBufferCount(long count) {
        builder.maxBufferCount(count);
        return this;
    }

    public GenericSimpleBufferTriggerBuilder<E, C> maxBufferCount(long count,
            Consumer<? super E> rejectHandler) {
        builder.maxBufferCount(count, rejectHandler);
        return this;
    }

    public GenericSimpleBufferTriggerBuilder<E, C>
            rejectHandler(Consumer<? super E> rejectHandler) {
        builder.rejectHandler(rejectHandler);
        return this;
    }

    public BufferTrigger<E> build() {
        return builder.build();
    }
}
