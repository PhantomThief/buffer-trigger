/**
 *
 */
package com.github.phantomthief.collection.impl;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import static java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.function.ToIntBiFunction;

import org.slf4j.Logger;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.collection.impl.SimpleBufferTrigger.TriggerStrategy.TriggerResult;
import com.github.phantomthief.util.ThrowableConsumer;

/**
 * @author w.vela
 */
public class SimpleBufferTrigger<E> implements BufferTrigger<E> {

    private static final long MIN_TICKER = SECONDS.toMillis(1);
    static Logger logger = getLogger(SimpleBufferTrigger.class);

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
    private final ReadLock readLock;
    private final WriteLock writeLock;

    private volatile long lastConsumeTimestamp = System.currentTimeMillis();

    SimpleBufferTrigger(Supplier<Object> bufferFactory, ToIntBiFunction<Object, E> queueAdder,
            ScheduledExecutorService scheduledExecutorService,
            ThrowableConsumer<Object, Throwable> consumer, TriggerStrategy triggerStrategy,
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
        this.buffer.set(this.bufferFactory.get());
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        readLock = lock.readLock();
        writeLock = lock.writeLock();
        scheduledExecutorService.schedule(
                new TriggerRunnable(triggerStrategy, scheduledExecutorService), MIN_TICKER,
                MILLISECONDS);
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
        boolean locked = false;
        try {
            readLock.lock();
            locked = true;
        } catch (Throwable e) {
            // ignore lock failed
        }
        int changedCount;
        try {
            Object thisBuffer = buffer.get();
            changedCount = queueAdder.applyAsInt(thisBuffer, element);
        } finally {
            if (locked) {
                readLock.unlock();
            }
        }
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
            writeLock.lock();
            try {
                old = buffer.getAndSet(bufferFactory.get());
            } finally {
                writeLock.unlock();
            }
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

    public interface TriggerStrategy {

        TriggerResult check(long lastConsumeTimestamp, long changedCount);

        class TriggerResult {

            private final boolean trigConsume;
            private final long nextTrigDuration;

            private TriggerResult(boolean trigConsume, long nextTrigTime) {
                this.trigConsume = trigConsume;
                this.nextTrigDuration = nextTrigTime;
            }

            public static TriggerResult next(boolean trigConsume, long interval, TimeUnit unit) {
                return new TriggerResult(trigConsume, unit.toMillis(interval));
            }

            public boolean trigConsume() {
                return trigConsume;
            }

            public long getNextTrigDuration() {
                return nextTrigDuration;
            }
        }
    }

    private class TriggerRunnable implements Runnable {

        private final TriggerStrategy triggerStrategy;
        private final ScheduledExecutorService scheduledExecutorService;

        TriggerRunnable(TriggerStrategy triggerStrategy,
                ScheduledExecutorService scheduledExecutorService) {
            this.triggerStrategy = triggerStrategy;
            this.scheduledExecutorService = scheduledExecutorService;
        }

        @Override
        public void run() {
            synchronized (SimpleBufferTrigger.this) {
                long nextTrigDuration = MIN_TICKER;
                try {
                    TriggerResult triggerResult = triggerStrategy.check(lastConsumeTimestamp,
                            counter.get());
                    if (triggerResult.getNextTrigDuration() > 0) {
                        nextTrigDuration = triggerResult.getNextTrigDuration();
                    }
                    if (triggerResult.trigConsume()) {
                        lastConsumeTimestamp = currentTimeMillis();
                        doConsume();
                    }
                } catch (Throwable e) {
                    logger.error("", e);
                }
                scheduledExecutorService.schedule(this, nextTrigDuration, MILLISECONDS);
            }
        }
    }
}
