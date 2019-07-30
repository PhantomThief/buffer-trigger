package com.github.phantomthief.collection.impl;

import static com.github.phantomthief.collection.impl.SimpleBufferTrigger.TriggerResult.trig;
import static com.github.phantomthief.util.MoreSuppliers.lazy;
import static java.lang.System.currentTimeMillis;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToIntBiFunction;

import javax.annotation.CheckReturnValue;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.collection.RejectHandler;
import com.github.phantomthief.collection.impl.SimpleBufferTrigger.TriggerResult;
import com.github.phantomthief.collection.impl.SimpleBufferTrigger.TriggerStrategy;
import com.github.phantomthief.util.ThrowableConsumer;

/**
 * @author w.vela
 */
public class GenericSimpleBufferTriggerBuilder<E, C> {

    private final SimpleBufferTriggerBuilder<Object, Object> builder;

    public GenericSimpleBufferTriggerBuilder(SimpleBufferTriggerBuilder<Object, Object> builder) {
        this.builder = builder;
    }

    /**
     * better use {@link #setContainerEx}
     *
     * @param queueAdder return if there is a change occurred.
     */
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> setContainer(Supplier<? extends C> factory,
            BiPredicate<? super C, ? super E> queueAdder) {
        builder.setContainer(factory, queueAdder);
        return this;
    }

    /**
     * @param queueAdder return the change size occurred.
     */
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> setContainerEx(Supplier<? extends C> factory,
            ToIntBiFunction<? super C, ? super E> queueAdder) {
        builder.setContainerEx(factory, queueAdder);
        return this;
    }

    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C>
            setScheduleExecutorService(ScheduledExecutorService scheduledExecutorService) {
        builder.setScheduleExecutorService(scheduledExecutorService);
        return this;
    }

    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C>
            setExceptionHandler(BiConsumer<? super Throwable, ? super C> exceptionHandler) {
        builder.setExceptionHandler(exceptionHandler);
        return this;
    }

    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C>
            triggerStrategy(TriggerStrategy triggerStrategy) {
        builder.triggerStrategy(triggerStrategy);
        return this;
    }

    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> intervalAtFixedRate(long interval,
            TimeUnit unit) {
        builder.triggerStrategy(new TriggerStrategy() {

            private final Supplier<Long> time = lazy(System::currentTimeMillis);

            /**
             * always align to the first trig time
             */
            @Override
            public TriggerResult canTrigger(long last, long change) {
                long alignTime = time.get();
                long intervalInMs = unit.toMillis(interval);
                long result = intervalInMs - (currentTimeMillis() - alignTime) % intervalInMs;
                return trig(change > 0, result);
            }
        });
        return this;
    }

    /**
     * use {@link #interval(long, TimeUnit)} or {@link #triggerStrategy}
     */
    @Deprecated
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> on(long interval, TimeUnit unit, long count) {
        builder.on(interval, unit, count);
        return this;
    }

    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> interval(long interval, TimeUnit unit) {
        builder.interval(interval, unit);
        return this;
    }

    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> interval(LongSupplier interval, TimeUnit unit) {
        builder.interval(interval, unit);
        return this;
    }

    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C>
            consumer(ThrowableConsumer<? super C, Throwable> consumer) {
        builder.consumer(consumer);
        return this;
    }

    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> maxBufferCount(long count) {
        builder.maxBufferCount(count);
        return this;
    }

    /**
     * disable internal switch lock for much more performances.
     * while it may make conflicts on enqueue and consumer logic.
     */
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> disableSwitchLock() {
        builder.disableSwitchLock();
        return this;
    }

    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> maxBufferCount(long count,
            Consumer<? super E> rejectHandler) {
        builder.maxBufferCount(count, rejectHandler);
        return this;
    }

    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> rejectHandler(Consumer<? super E> rejectHandler) {
        builder.rejectHandler(rejectHandler);
        return this;
    }

    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> rejectHandlerEx(RejectHandler<? super E> rejectHandler) {
        builder.rejectHandlerEx(rejectHandler);
        return this;
    }

    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> enableBackPressure() {
        builder.enableBackPressure();
        return this;
    }

    /**
     * use for debug and stats, like trigger thread's name.
     */
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> name(String name) {
        builder.name(name);
        return this;
    }

    public BufferTrigger<E> build() {
        return builder.build();
    }
}
