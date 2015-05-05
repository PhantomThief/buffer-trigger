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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.github.phantomthief.collection.BufferTrigger;

/**
 * @author w.vela
 */
public class SimpleBufferTrigger<E> implements BufferTrigger<E> {

    /**
     * trigger like redis's rdb
     * 
     * save 900 1
     * save 300 10
     * save 60 10000
     * 
     */

    private final AtomicLong counter = new AtomicLong();
    private final Consumer<Object> consumer;
    private final BiPredicate<Object, E> queueAdder;
    private final Supplier<Object> bufferFactory;
    private final BiConsumer<Throwable, Object> exceptionHandler;
    private final AtomicReference<Object> buffer = new AtomicReference<>();

    private volatile boolean running;

    private SimpleBufferTrigger(Supplier<Object> bufferFactory, BiPredicate<Object, E> queueAdder,
            ScheduledExecutorService scheduledExecutorService, Consumer<Object> consumer,
            Map<Long, Long> triggerMap, BiConsumer<Throwable, Object> exceptionHandler) {
        this.queueAdder = queueAdder;
        this.bufferFactory = bufferFactory;
        this.consumer = consumer;
        this.exceptionHandler = exceptionHandler;
        for (Entry<Long, Long> entry : triggerMap.entrySet()) {
            scheduledExecutorService.scheduleWithFixedDelay(() -> {
                synchronized (SimpleBufferTrigger.this) {
                    if (running) {
                        return;
                    }
                    if (counter.get() < entry.getValue()) {
                        return;
                    }
                    running = true;
                    Object old = null;
                    try {
                        old = buffer.getAndSet(bufferFactory.get());
                        counter.set(0);
                        consumer.accept(old);
                    } catch (Throwable e) {
                        if (this.exceptionHandler != null) {
                            try {
                                this.exceptionHandler.accept(e, old);
                            } catch (Throwable idontcare) {
                                // do nothing
                            }
                        } else {
                            e.printStackTrace();
                        }
                    } finally {
                        running = false;
                    }
                }
            } , entry.getKey(), entry.getKey(), TimeUnit.MILLISECONDS);
        }
    }

    /* (non-Javadoc)
     * @see com.github.phantomthief.collection.BufferTrigger#enqueue(java.lang.Object)
     */
    @Override
    public void enqueue(E element) {
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
            running = true;
            Object old = null;
            try {
                old = buffer.getAndSet(bufferFactory.get());
                counter.set(0);
                consumer.accept(old);
            } catch (Throwable e) {
                if (this.exceptionHandler != null) {
                    try {
                        this.exceptionHandler.accept(e, old);
                    } catch (Throwable idontcare) {
                        // do nothing
                    }
                } else {
                    e.printStackTrace();
                }
            } finally {
                running = false;
            }
        }
    }

    public static class Builder<E, C> {

        private ScheduledExecutorService scheduledExecutorService;
        private Supplier<C> bufferFactory;
        private BiPredicate<C, E> queueAdder;
        private Consumer<C> consumer;
        private BiConsumer<Throwable, C> exceptionHandler;
        private final Map<Long, Long> triggerMap = new HashMap<>();

        /**
         * <b>warning:</b> the container must be thread-safed.
         * 
         * @param factory
         * @param queueAdder
         * @return
         */
        public Builder<E, C> setContainer(Supplier<C> factory, BiPredicate<C, E> queueAdder) {
            if (factory == null || queueAdder == null) {
                throw new IllegalArgumentException();
            }
            this.bufferFactory = factory;
            this.queueAdder = queueAdder;
            return this;
        }

        public Builder<E, C> setScheduleExecutorService(
                ScheduledExecutorService scheduledExecutorService) {
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

        public Builder<E, C> consumer(Consumer<C> consumer) {
            this.consumer = consumer;
            return this;
        }

        @SuppressWarnings("unchecked")
        public SimpleBufferTrigger<E> build() {
            ensure();
            return new SimpleBufferTrigger<E>((Supplier<Object>) bufferFactory,
                    (BiPredicate<Object, E>) queueAdder, scheduledExecutorService,
                    (Consumer<Object>) consumer, triggerMap,
                    (BiConsumer<Throwable, Object>) exceptionHandler);
        }

        @SuppressWarnings("unchecked")
        private void ensure() {
            if (consumer == null) {
                throw new IllegalArgumentException("there is no consumer defined.");
            }
            if (bufferFactory == null) {
                bufferFactory = () -> (C) Collections.synchronizedSet(new HashSet<>());
            }
            if (queueAdder == null) {
                queueAdder = (c, e) -> ((Set<E>) c).add(e);
            }
            if (scheduledExecutorService == null) {
                scheduledExecutorService = makeScheduleExecutor();
            }
        }

        private ScheduledExecutorService makeScheduleExecutor() {
            ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(0);

            ((ScheduledThreadPoolExecutor) scheduledExecutorService)
                    .setThreadFactory(new ThreadFactory() {

                        private final ThreadGroup group;
                        private final AtomicInteger threadNumber = new AtomicInteger(1);
                        private final String namePrefix;

                        {
                            SecurityManager s = System.getSecurityManager();
                            group = (s != null) ? s.getThreadGroup()
                                    : Thread.currentThread().getThreadGroup();
                            namePrefix = "pool-simple-buffer-trigger-thread-";
                        }

                        @Override
                        public Thread newThread(Runnable r) {
                            Thread t = new Thread(group, r,
                                    namePrefix + threadNumber.getAndIncrement(), 0);
                            if (t.isDaemon()) {
                                t.setDaemon(false);
                            }
                            if (t.getPriority() != Thread.NORM_PRIORITY) {
                                t.setPriority(Thread.NORM_PRIORITY);
                            }
                            return t;
                        }
                    });
            return scheduledExecutorService;
        }

    }

    public static final <E, C> Builder<E, C> newBuilder() {
        return new Builder<>();
    };

}
