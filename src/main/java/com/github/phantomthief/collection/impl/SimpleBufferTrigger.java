/**
 *
 */
package com.github.phantomthief.collection.impl;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import static java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToIntBiFunction;

import org.slf4j.Logger;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.util.ThrowableConsumer;

/**
 * @author w.vela
 */
public class SimpleBufferTrigger<E> implements BufferTrigger<E> {

    private static final Logger logger = getLogger(SimpleBufferTrigger.class);

    private final AtomicLong counter = new AtomicLong();
    private final ThrowableConsumer<Object, Throwable> consumer;
    private final ToIntBiFunction<Object, E> queueAdder;
    private final Supplier<Object> bufferFactory;
    private final BiConsumer<Throwable, Object> exceptionHandler;
    private final AtomicReference<Object> buffer = new AtomicReference<>();
    private final long maxBufferCount;
    private final Consumer<E> rejectHandler;
    private final ReadLock readLock;
    private final WriteLock writeLock;

    private volatile long lastConsumeTimestamp = System.currentTimeMillis();

    SimpleBufferTrigger(Supplier<Object> bufferFactory, ToIntBiFunction<Object, E> queueAdder,
            ScheduledExecutorService scheduledExecutorService,
            ThrowableConsumer<Object, Throwable> consumer, long tickTime,
            TriggerStrategy triggerStrategy, BiConsumer<Throwable, Object> exceptionHandler,
            long maxBufferCount, Consumer<E> rejectHandler) {
        this.queueAdder = queueAdder;
        this.bufferFactory = bufferFactory;
        this.consumer = consumer;
        this.exceptionHandler = exceptionHandler;
        this.maxBufferCount = maxBufferCount;
        this.rejectHandler = rejectHandler;
        this.buffer.set(this.bufferFactory.get());
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        readLock = lock.readLock();
        writeLock = lock.writeLock();
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            synchronized (SimpleBufferTrigger.this) {
                try {
                    if (triggerStrategy.canTrigger(lastConsumeTimestamp, counter.get())) {
                        lastConsumeTimestamp = currentTimeMillis();
                        doConsume();
                    }
                } catch (Throwable e) {
                    logger.error("", e);
                }
            }
        }, tickTime, tickTime, MILLISECONDS);
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
        if (maxBufferCount > 0 && currentCount >= maxBufferCount) {
            if (rejectHandler != null) {
                rejectHandler.accept(element);
            }
            return;
        }
        boolean locked = false;
        try {
            readLock.lock();
            locked = true;
        } catch (Throwable e) {
            // ignore lock failed
        }
        try {
            Object thisBuffer = buffer.get();
            int changedCount = queueAdder.applyAsInt(thisBuffer, element);
            if (changedCount > 0) {
                counter.addAndGet(changedCount);
            }
        } finally {
            if (locked) {
                readLock.unlock();
            }
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
            writeLock.lock();
            try {
                old = buffer.getAndSet(bufferFactory.get());
            } finally {
                counter.set(0);
                writeLock.unlock();
            }
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

    public interface TriggerStrategy {

        boolean canTrigger(long lastConsumeTimestamp, long changedCount);
    }
}
