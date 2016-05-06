/**
 * 
 */
package com.github.phantomthief.collection.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.function.ToIntBiFunction;

import org.slf4j.Logger;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.util.ThrowableConsumer;

/**
 * @author w.vela
 */
public class SimpleBufferTrigger<E> implements BufferTrigger<E> {

    static Logger logger = getLogger(SimpleBufferTrigger.class);

    /**
     * trigger like redis's rdb
     * 
     * save 900 1
     * save 300 10
     * save 60 10000
     * 
     */

    private final AtomicLong counter = new AtomicLong();
    private final ThrowableConsumer<Object, Throwable> consumer;
    private final ToIntBiFunction<Object, E> queueAdder;
    private final Supplier<Object> bufferFactory;
    private final BiConsumer<Throwable, Object> exceptionHandler;
    private final AtomicReference<Object> buffer = new AtomicReference<>();
    private final long maxBufferCount;
    private final long warningBufferThreshold;
    private final LongConsumer warningBufferHandler;
    private final Consumer<E> rejectHandler;

    SimpleBufferTrigger(Supplier<Object> bufferFactory, ToIntBiFunction<Object, E> queueAdder,
            ScheduledExecutorService scheduledExecutorService,
            ThrowableConsumer<Object, Throwable> consumer, Map<Long, Long> triggerMap,
            BiConsumer<Throwable, Object> exceptionHandler, long maxBufferCount,
            Consumer<E> rejectHandler, long warningBufferThreshold,
            LongConsumer warningBufferHandler) {
        this.queueAdder = queueAdder;
        this.bufferFactory = bufferFactory;
        this.consumer = consumer;
        this.exceptionHandler = exceptionHandler;
        this.maxBufferCount = maxBufferCount;
        this.rejectHandler = rejectHandler;
        this.warningBufferHandler = warningBufferHandler;
        this.warningBufferThreshold = warningBufferThreshold;
        for (Entry<Long, Long> entry : triggerMap.entrySet()) {
            scheduledExecutorService.scheduleWithFixedDelay(() -> {
                synchronized (SimpleBufferTrigger.this) {
                    if (counter.get() < entry.getValue()) {
                        return;
                    }
                    doConsume();
                }
            }, entry.getKey(), entry.getKey(), MILLISECONDS);
        }
    }

    public static SimpleBufferTriggerBuilder<Object, Object> newBuilder() {
        return new SimpleBufferTriggerBuilder<>();
    }

    public static <E, C> GenericSimpleBufferTriggerBuilder<E, C> newGenericBuilder() {
        return new GenericSimpleBufferTriggerBuilder<>(newBuilder());
    }

    public static SimpleBufferTriggerBuilder<Object, Map<Object, Integer>> newCounterBuilder() {
        return new SimpleBufferTriggerBuilder<Object, Map<Object, Integer>>() //
                .setContainer(ConcurrentHashMap::new, (map, element) -> {
                    map.merge(element, 1, Math::addExact);
                    return true;
                });
    }

    @Override
    public void enqueue(E element) {
        long currentCount = counter.get();
        if (warningBufferThreshold > 0 && maxBufferCount > 0 && warningBufferHandler != null) {
            if (currentCount >= warningBufferThreshold) {
                warningBufferHandler.accept(currentCount);
            }
        }
        if (maxBufferCount > 0 && currentCount >= maxBufferCount) {
            if (rejectHandler != null) {
                rejectHandler.accept(element);
            }
            return;
        }
        Object thisBuffer = buffer.updateAndGet(old -> old != null ? old : bufferFactory.get());
        int changedCount = queueAdder.applyAsInt(thisBuffer,element);
        if (changedCount > 0) {
            counter.addAndGet(changedCount);
        }

    }

    @Override
    public void manuallyDoTrigger() {
        synchronized (SimpleBufferTrigger.this) {
            doConsume();
        }
    }

    private void doConsume() {
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
                    e.printStackTrace();
                    idontcare.printStackTrace();
                }
            } else {
                logger.error("Ops.", e);
            }
        }
    }

    @Override
    public long getPendingChanges() {
        return counter.get();
    }
}
