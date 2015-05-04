/**
 * 
 */
package com.github.phantomthief.trigger.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.github.phantomthief.trigger.Trigger;

/**
 * @author w.vela
 */
public class ScheduledTrigger implements Trigger {

    private final class ScheduledHolder {

        private final long interval;
        private final Runnable runnable;
        private final long triggerThreshold;

        private volatile boolean running = false;

        /**
         * @param interval
         * @param runnable
         * @param triggerThreshold
         */
        public ScheduledHolder(long interval, Runnable runnable, long triggerThreshold) {
            this.interval = interval;
            this.runnable = runnable;
            this.triggerThreshold = triggerThreshold;
        }

        private final AtomicLong counter = new AtomicLong();
        private volatile ScheduledFuture<?> scheduled;

        public void doAction() {
            synchronized (runnable) {
                running = true;
                try {
                    if (scheduled != null) {
                        scheduled.cancel(true);
                        scheduled = null;
                    }
                    runnable.run();
                    counter.set(0);
                } finally {
                    running = false;
                }
            }
        }

        public void count() {
            if (scheduled == null) {
                synchronized (runnable) {
                    if (scheduled == null) {
                        scheduled = scheduledExecutorService.schedule(() -> {
                            synchronized (runnable) {
                                if (Thread.interrupted()) {
                                    return;
                                }
                                running = true;
                                try {
                                    runnable.run();
                                    scheduled = null;
                                    counter.set(0);
                                } finally {
                                    running = false;
                                }
                            }
                        } , interval, TimeUnit.MILLISECONDS);
                    }
                }
            }
            if (triggerThreshold > 0) {
                synchronized (runnable) {
                    long incrementAndGet = counter.incrementAndGet();
                    if (incrementAndGet > triggerThreshold && !running) {
                        counter.set(0);
                        scheduledExecutorService.execute(() -> {
                            synchronized (runnable) {
                                running = true;
                                try {
                                    if (scheduled != null) {
                                        scheduled.cancel(true);
                                        scheduled = null;
                                    }
                                    runnable.run();
                                } finally {
                                    running = false;
                                }
                            }
                        } );
                    }
                }
            }
        }

    }

    private final ScheduledExecutorService scheduledExecutorService = Executors
            .newScheduledThreadPool(0);

    {

        ((ScheduledThreadPoolExecutor) scheduledExecutorService)
                .setThreadFactory(new ThreadFactory() {

                    private final ThreadGroup group;
                    private final AtomicInteger threadNumber = new AtomicInteger(1);
                    private final String namePrefix;

                    {
                        SecurityManager s = System.getSecurityManager();
                        group = (s != null) ? s.getThreadGroup()
                                : Thread.currentThread().getThreadGroup();
                        namePrefix = "pool-buffer-trigger-thread-";
                    }

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(),
                                0);
                        if (t.isDaemon()) {
                            t.setDaemon(false);
                        }
                        if (t.getPriority() != Thread.NORM_PRIORITY) {
                            t.setPriority(Thread.NORM_PRIORITY);
                        }
                        return t;
                    }
                });
    }

    private final ConcurrentMap<Long, ScheduledHolder> triggerMap = new ConcurrentHashMap<>();

    private final Map<Long, Entry<Long, Runnable>> triggerDefineMap;

    /**
     * @param action
     * @param triggerDefineMap
     */
    private ScheduledTrigger(Map<Long, Entry<Long, Runnable>> triggerDefineMap) {
        this.triggerDefineMap = triggerDefineMap;
    }

    /* (non-Javadoc)
     * @see com.github.phantomthief.trigger.Trigger#markChange()
     */
    @Override
    public void markChange() {
        for (Entry<Long, Entry<Long, Runnable>> entry : triggerDefineMap.entrySet()) {
            ScheduledHolder counter = triggerMap.computeIfAbsent(entry.getKey(),
                    n -> new ScheduledHolder(entry.getKey(), entry.getValue().getValue(),
                            entry.getValue().getKey()));
            counter.count();
        }
    }

    /* (non-Javadoc)
     * @see com.github.phantomthief.trigger.Trigger#doAction()
     */
    @Override
    public void doAction() {
        for (Entry<Long, ScheduledHolder> entry : triggerMap.entrySet()) {
            entry.getValue().doAction();
        }
    }

    public static final TriggerBuilder newBuilder() {
        return new TriggerBuilder();
    }

    public static class TriggerBuilder {

        private final Map<Long, Entry<Long, Runnable>> triggerMap = new HashMap<>();

        public TriggerBuilder on(long interval, TimeUnit unit, long count, Runnable function) {
            triggerMap.put(unit.toMillis(interval), new EntryImpl<>(count, function));
            return this;
        }

        public TriggerBuilder fixedRate(long interval, TimeUnit unit, Runnable function) {
            triggerMap.put(unit.toMillis(interval), new EntryImpl<>(-1L, function));
            return this;
        }

        public ScheduledTrigger build() {
            return new ScheduledTrigger(triggerMap);
        }

    }

    /**
     * 
     * @author w.vela
     */
    private static final class EntryImpl<K, V> implements Map.Entry<K, V> {

        private final K key;

        private V value;

        /**
         * @param key
         * @param value
         */
        private EntryImpl(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            this.value = value;
            return value;
        }

        @Override
        public String toString() {
            return key + "=" + value;
        }

        @Override
        public final int hashCode() {
            return Objects.hashCode(key) ^ Objects.hashCode(value);
        }

        @Override
        public final boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o instanceof Map.Entry) {
                Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
                if (Objects.equals(key, e.getKey()) && Objects.equals(value, e.getValue())) {
                    return true;
                }
            }
            return false;
        }

    }

}
