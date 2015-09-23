/**
 * 
 */
package com.github.phantomthief.collection.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.collection.ThrowingConsumer;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author w.vela
 */
public class SimpleBufferTrigger<E> implements BufferTrigger<E> {

    private static org.slf4j.Logger logger = org.slf4j.LoggerFactory
            .getLogger(SimpleBufferTrigger.class);

    /**
     * trigger like redis's rdb
     * 
     * save 900 1
     * save 300 10
     * save 60 10000
     * 
     */

    private final AtomicLong counter = new AtomicLong();
    private final ThrowingConsumer<Object> consumer;
    private final BiPredicate<Object, E> queueAdder;
    private final Supplier<Object> bufferFactory;
    private final BiConsumer<Throwable, Object> exceptionHandler;
    private final AtomicReference<Object> buffer = new AtomicReference<>();
    private final long maxBufferCount;
    private final Consumer<E> rejectHandler;

    private SimpleBufferTrigger(Supplier<Object> bufferFactory, BiPredicate<Object, E> queueAdder,
            ScheduledExecutorService scheduledExecutorService, ThrowingConsumer<Object> consumer,
            Map<Long, Long> triggerMap, BiConsumer<Throwable, Object> exceptionHandler,
            long maxBufferCount, Consumer<E> rejectHandler) {
        this.queueAdder = queueAdder;
        this.bufferFactory = bufferFactory;
        this.consumer = consumer;
        this.exceptionHandler = exceptionHandler;
        this.maxBufferCount = maxBufferCount;
        this.rejectHandler = rejectHandler;
        for (Entry<Long, Long> entry : triggerMap.entrySet()) {
            scheduledExecutorService.scheduleWithFixedDelay(() -> {
                synchronized (SimpleBufferTrigger.this) {
                    if (counter.get() < entry.getValue()) {
                        return;
                    }
                    Object old = null;
                    try {
                        old = buffer.getAndSet(bufferFactory.get());
                        counter.set(0);
                        if (old != null) {
                            consumer.acceptThrows(old);
                        }
                    } catch (Throwable e) {
                        if (this.exceptionHandler != null) {
                            try {
                                this.exceptionHandler.accept(e, old);
                            } catch (Throwable idontcare) {
                                // do nothing
                            }
                        } else {
                            logger.error("Ops.", e);
                        }
                    }
                }
            } , entry.getKey(), entry.getKey(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void enqueue(E element) {
        if (maxBufferCount > 0 && counter.get() >= maxBufferCount) {
            if (rejectHandler != null) {
                rejectHandler.accept(element);
            }
            return;
        }
        Object thisBuffer = buffer.updateAndGet(old -> old != null ? old : bufferFactory.get());
        boolean addSuccess = queueAdder.test(thisBuffer, element);
        if (addSuccess) {
            counter.incrementAndGet();
        }

    }

    /* (non-Javadoc)
     * @see com.github.phantomthief.collection.BufferTrigger#manuallyDoTrigger()
     */
    @Override
    public void manuallyDoTrigger() {
        synchronized (SimpleBufferTrigger.this) {
            Object old = null;
            try {
                old = buffer.getAndSet(bufferFactory.get());
                counter.set(0);
                if (old != null) {
                    consumer.accept(old);
                }
            } catch (Throwable e) {
                if (this.exceptionHandler != null) {
                    try {
                        this.exceptionHandler.accept(e, old);
                    } catch (Throwable idontcare) {
                        // do nothing
                    }
                } else {
                    logger.error("Ops.", e);
                }
            }
        }
    }

    public static class Builder<E, C> {

        private ScheduledExecutorService scheduledExecutorService;
        private Supplier<C> bufferFactory;
        private BiPredicate<C, E> queueAdder;
        private ThrowingConsumer<C> consumer;
        private BiConsumer<Throwable, C> exceptionHandler;
        private long maxBufferCount = -1;
        private Consumer<E> rejectHandler;
        private final Map<Long, Long> triggerMap = new HashMap<>();

        /**
         * <b>warning:</b> the container must be thread-safed.
         * 
         * @param factory
         * @param queueAdder
         * @return
         */
        public Builder<E, C> setContainer(Supplier<C> factory, BiPredicate<C, E> queueAdder) {
            Preconditions.checkNotNull(factory);
            Preconditions.checkNotNull(queueAdder);

            this.bufferFactory = factory;
            this.queueAdder = queueAdder;
            return this;
        }

        public Builder<E, C>
                setScheduleExecutorService(ScheduledExecutorService scheduledExecutorService) {
            this.scheduledExecutorService = scheduledExecutorService;
            return this;
        }

        public Builder<E, C> setExceptionHandler(BiConsumer<Throwable, C> exceptionHandler) {
            this.exceptionHandler = exceptionHandler;
            return this;
        }

        public Builder<E, C> on(long interval, TimeUnit unit, long count) {
            triggerMap.put(unit.toMillis(interval), count);
            return this;
        }

        public Builder<E, C> consumer(ThrowingConsumer<C> consumer) {
            this.consumer = consumer;
            return this;
        }

        /**
         * it's better dealing this in container
         */
        public Builder<E, C> maxBufferCount(long count) {
            this.maxBufferCount = count;
            return this;
        }

        /**
         * it's better dealing this in container
         */
        public Builder<E, C> rejectHandler(Consumer<E> rejectHandler) {
            this.rejectHandler = rejectHandler;
            return this;
        }

        @SuppressWarnings("unchecked")
        public SimpleBufferTrigger<E> build() {
            ensure();
            return new SimpleBufferTrigger<E>((Supplier<Object>) bufferFactory,
                    (BiPredicate<Object, E>) queueAdder, scheduledExecutorService,
                    (ThrowingConsumer<Object>) consumer, triggerMap,
                    (BiConsumer<Throwable, Object>) exceptionHandler, maxBufferCount,
                    rejectHandler);
        }

        @SuppressWarnings("unchecked")
        private void ensure() {
            Preconditions.checkNotNull(consumer);

            if (bufferFactory == null) {
                bufferFactory = () -> (C) Collections.synchronizedSet(new HashSet<>());
            }
            if (queueAdder == null) {
                queueAdder = (c, e) -> ((Set<E>) c).add(e);
            }
            if (!triggerMap.isEmpty() && scheduledExecutorService == null) {
                scheduledExecutorService = makeScheduleExecutor();
            }
        }

        private ScheduledExecutorService makeScheduleExecutor() {
            ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(
                    Math.max(1, triggerMap.size()), new ThreadFactoryBuilder()
                            .setNameFormat("pool-simple-buffer-trigger-thread-%d").build());

            return scheduledExecutorService;
        }
    }

    public static final <E, C> Builder<E, C> newBuilder() {
        return new Builder<>();
    }

    public static final <E> Builder<E, Map<E, Integer>> newCounterBuilder() {
        return new Builder<E, Map<E, Integer>>() //
                .setContainer(ConcurrentHashMap::new, (map, element) -> {
                    map.merge(element, 1,
                            (oldValue, appendValue) -> oldValue == null ? appendValue : oldValue
                                    + appendValue);
                    return true;
                });
    }
}
