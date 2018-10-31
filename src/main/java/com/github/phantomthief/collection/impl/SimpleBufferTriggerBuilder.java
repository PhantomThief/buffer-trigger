package com.github.phantomthief.collection.impl;

import static com.github.phantomthief.collection.impl.SimpleBufferTrigger.TriggerResult.empty;
import static com.github.phantomthief.collection.impl.SimpleBufferTrigger.TriggerResult.trig;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToIntBiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.collection.impl.SimpleBufferTrigger.TriggerStrategy;
import com.github.phantomthief.util.ThrowableConsumer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

@SuppressWarnings("unchecked")
public class SimpleBufferTriggerBuilder<E, C> {

    private static final Logger logger = LoggerFactory.getLogger(SimpleBufferTriggerBuilder.class);

    private TriggerStrategy triggerStrategy;
    private ScheduledExecutorService scheduledExecutorService;
    private Supplier<C> bufferFactory;
    private ToIntBiFunction<C, E> queueAdder;
    private ThrowableConsumer<C, Throwable> consumer;
    private BiConsumer<Throwable, C> exceptionHandler;
    private long maxBufferCount = -1;
    private Consumer<E> rejectHandler;
    private String name;

    /**
     * <b>warning:</b> the container must be thread-safed.
     * better use {@link #setContainerEx}
     *
     * @param queueAdder return if there is a change occurred.
     */
    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1> setContainer(Supplier<? extends C1> factory,
            BiPredicate<? super C1, ? super E1> queueAdder) {
        checkNotNull(factory);
        checkNotNull(queueAdder);

        SimpleBufferTriggerBuilder<E1, C1> thisBuilder = (SimpleBufferTriggerBuilder<E1, C1>) this;
        thisBuilder.bufferFactory = (Supplier<C1>) factory;
        thisBuilder.queueAdder = (c, e) -> queueAdder.test(c, e) ? 1 : 0;
        return thisBuilder;
    }

    /**
     * <b>warning:</b> the container must be thread-safed.
     *
     * @param queueAdder return the change size occurred.
     */
    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1> setContainerEx(
            Supplier<? extends C1> factory, ToIntBiFunction<? super C1, ? super E1> queueAdder) {
        checkNotNull(factory);
        checkNotNull(queueAdder);

        SimpleBufferTriggerBuilder<E1, C1> thisBuilder = (SimpleBufferTriggerBuilder<E1, C1>) this;
        thisBuilder.bufferFactory = (Supplier<C1>) factory;
        thisBuilder.queueAdder = (ToIntBiFunction<C1, E1>) queueAdder;
        return thisBuilder;
    }

    public SimpleBufferTriggerBuilder<E, C>
            setScheduleExecutorService(ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
        return this;
    }

    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1>
            setExceptionHandler(BiConsumer<? super Throwable, ? super C1> exceptionHandler) {
        SimpleBufferTriggerBuilder<E1, C1> thisBuilder = (SimpleBufferTriggerBuilder<E1, C1>) this;
        thisBuilder.exceptionHandler = (BiConsumer<Throwable, C1>) exceptionHandler;
        return thisBuilder;
    }

    public SimpleBufferTriggerBuilder<E, C> triggerStrategy(TriggerStrategy triggerStrategy) {
        this.triggerStrategy = triggerStrategy;
        return this;
    }

    /**
     * use {@link #interval(long, TimeUnit)} or {@link #triggerStrategy}
     */
    @Deprecated
    public SimpleBufferTriggerBuilder<E, C> on(long interval, TimeUnit unit, long count) {
        if (triggerStrategy == null) {
            triggerStrategy = new MultiIntervalTriggerStrategy();
        }
        if (triggerStrategy instanceof MultiIntervalTriggerStrategy) {
            ((MultiIntervalTriggerStrategy) triggerStrategy).on(interval, unit, count);
        } else {
            logger.warn(
                    "exists non multi interval trigger strategy found. ignore setting:{},{}->{}",
                    interval, unit, count);
        }
        return this;
    }

    public SimpleBufferTriggerBuilder<E, C> interval(long interval, TimeUnit unit) {
        return interval(() -> interval, unit);
    }

    public SimpleBufferTriggerBuilder<E, C> interval(LongSupplier interval, TimeUnit unit) {
        this.triggerStrategy = (last, change) -> {
            long intervalInMs = unit.toMillis(interval.getAsLong());
            return trig(change > 0 && currentTimeMillis() - last >= intervalInMs, intervalInMs);
        };
        return this;
    }

    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1>
            consumer(ThrowableConsumer<? super C1, Throwable> consumer) {
        checkNotNull(consumer);
        SimpleBufferTriggerBuilder<E1, C1> thisBuilder = (SimpleBufferTriggerBuilder<E1, C1>) this;
        thisBuilder.consumer = (ThrowableConsumer<C1, Throwable>) consumer;
        return thisBuilder;
    }

    /**
     * it's better dealing this in container
     */
    public SimpleBufferTriggerBuilder<E, C> maxBufferCount(long count) {
        checkArgument(count > 0);

        this.maxBufferCount = count;
        return this;
    }

    /**
     * it's better dealing this in container
     */
    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1> maxBufferCount(long count,
            Consumer<? super E1> rejectHandler) {
        return (SimpleBufferTriggerBuilder<E1, C1>) maxBufferCount(count)
                .rejectHandler(rejectHandler);
    }

    /**
     * it's better dealing this in container
     */
    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1>
            rejectHandler(Consumer<? super E1> rejectHandler) {
        checkNotNull(rejectHandler);
        SimpleBufferTriggerBuilder<E1, C1> thisBuilder = (SimpleBufferTriggerBuilder<E1, C1>) this;
        thisBuilder.rejectHandler = (Consumer<E1>) rejectHandler;
        return thisBuilder;
    }

    /**
     * use for debug and stats, like trigger thread's name.
     */
    public SimpleBufferTriggerBuilder<E, C> name(String name) {
        this.name = name;
        return this;
    }

    public <E1> BufferTrigger<E1> build() {
        return new LazyBufferTrigger<>(() -> {
            ensure();
            return new SimpleBufferTrigger<>((Supplier<Object>) bufferFactory,
                    (ToIntBiFunction<Object, E1>) queueAdder, scheduledExecutorService,
                    (ThrowableConsumer<Object, Throwable>) consumer, triggerStrategy,
                    (BiConsumer<Throwable, Object>) exceptionHandler, maxBufferCount,
                    (Consumer<E1>) rejectHandler);
        });
    }

    private void ensure() {
        checkNotNull(consumer);

        if (triggerStrategy == null) {
            logger.warn("no trigger strategy found. using NO-OP trigger");
            triggerStrategy = (t, n) -> empty();
        }

        if (bufferFactory == null && queueAdder == null) {
            bufferFactory = () -> (C) newSetFromMap(new ConcurrentHashMap<>());
            queueAdder = (c, e) -> ((Set<E>) c).add(e) ? 1 : 0;
        }
        if (scheduledExecutorService == null) {
            scheduledExecutorService = makeScheduleExecutor();
        }
    }

    private ScheduledExecutorService makeScheduleExecutor() {
        String threadPattern = name == null ? "pool-simple-buffer-trigger-thread-%d" : "pool-simple-buffer-trigger-thread-["
                + name + "]";
        return newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat(threadPattern) //
                        .setDaemon(true) //
                        .build());
    }
}