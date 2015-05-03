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
import java.util.concurrent.TimeUnit;
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
            synchronized (this) {
                if (scheduled != null) {
                    scheduled.cancel(true);
                    scheduled = null;
                }
                runnable.run();
                counter.set(0);
            }
        }

        public void count() {
            if (scheduled == null) {
                synchronized (this) {
                    if (scheduled == null) {
                        scheduled = scheduledExecutorService.schedule(() -> {
                            synchronized (ScheduledHolder.this) {
                                if (Thread.interrupted()) {
                                    return;
                                }
                                runnable.run();
                                scheduled = null;
                                counter.set(0);
                            }
                        } , interval, TimeUnit.MILLISECONDS);
                    }
                }
            }
            if (triggerThreshold > 0) {
                long incrementAndGet = counter.incrementAndGet();
                if (incrementAndGet > triggerThreshold) {
                    synchronized (this) {
                        if (scheduled != null) {
                            scheduled.cancel(true);
                            scheduled = null;
                        }
                        runnable.run();
                        counter.set(0);
                    }
                }
            }
        }

    }

    private final ScheduledExecutorService scheduledExecutorService = Executors
            .newScheduledThreadPool(0);
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
