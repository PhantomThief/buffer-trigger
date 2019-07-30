package com.github.phantomthief.collection.impl;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import static java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.function.ToIntBiFunction;

import org.slf4j.Logger;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.util.ThrowableConsumer;

/**
 * @author w.vela
 */
public class SimpleBufferTrigger<E, C> implements BufferTrigger<E> {

    private static final Logger logger = getLogger(SimpleBufferTrigger.class);

    private static final long DEFAULT_NEXT_TRIGGER_PERIOD = TimeUnit.SECONDS.toMillis(1);

    private final AtomicLong counter = new AtomicLong();
    private final ThrowableConsumer<C, Throwable> consumer;
    private final ToIntBiFunction<C, E> queueAdder;
    private final Supplier<C> bufferFactory;
    private final BiConsumer<Throwable, C> exceptionHandler;
    private final AtomicReference<C> buffer = new AtomicReference<>();
    private final long maxBufferCount;
    private final RejectHandler<E> rejectHandler;
    private final ReadLock readLock;
    private final WriteLock writeLock;
    private final Condition writeCondition;

    private volatile long lastConsumeTimestamp = currentTimeMillis();

    SimpleBufferTrigger(SimpleBufferTriggerBuilder<E, C> builder) {
        this.queueAdder = builder.queueAdder;
        this.bufferFactory = builder.bufferFactory;
        this.consumer = builder.consumer;
        this.exceptionHandler = builder.exceptionHandler;
        this.maxBufferCount = builder.maxBufferCount;
        this.rejectHandler = builder.rejectHandler;
        this.buffer.set(this.bufferFactory.get());
        if (!builder.disableSwitchLock) {
            ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
            readLock = lock.readLock();
            writeLock = lock.writeLock();
            writeCondition = writeLock.newCondition();
        } else {
            readLock = null;
            writeLock = null;
            writeCondition = null;
        }
        builder.scheduledExecutorService.schedule(
                new TriggerRunnable(builder.scheduledExecutorService, builder.triggerStrategy),
                DEFAULT_NEXT_TRIGGER_PERIOD, MILLISECONDS);
    }

    /**
     * use {@link com.github.phantomthief.collection.BufferTrigger#simple} instead
     * or {@link com.github.phantomthief.collection.BufferTrigger#simpleTrigger}
     */
    @Deprecated
    public static SimpleBufferTriggerBuilder<Object, Object> newBuilder() {
        return new SimpleBufferTriggerBuilder<>();
    }

    /**
     * use {@link com.github.phantomthief.collection.BufferTrigger#simple} instead
     */
    @Deprecated
    public static <E, C> GenericSimpleBufferTriggerBuilder<E, C> newGenericBuilder() {
        return new GenericSimpleBufferTriggerBuilder<>(newBuilder());
    }

    public static SimpleBufferTriggerBuilder<Object, Map<Object, Integer>> newCounterBuilder() {
        return new SimpleBufferTriggerBuilder<Object, Map<Object, Integer>>()
                .setContainer(ConcurrentHashMap::new, (map, element) -> {
                    map.merge(element, 1, Math::addExact);
                    return true;
                });
    }

    @Override
    public void enqueue(E element) {
        long currentCount = counter.get();
        if (maxBufferCount > 0 && currentCount >= maxBufferCount) {
            boolean pass = fireRejectHandler(element);
            if (!pass) {
                return;
            }
        }
        boolean locked = false;
        if (readLock != null) {
            try {
                readLock.lock();
                locked = true;
            } catch (Throwable e) {
                // ignore lock failed
            }
        }
        try {
            C thisBuffer = buffer.get();
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

    private boolean fireRejectHandler(E element) {
        boolean pass = false;
        if (rejectHandler != null) {
            if (writeLock != null && writeCondition != null) {
                writeLock.lock();
            }
            try {
                pass = rejectHandler.onReject(element, writeCondition);
            } catch (Throwable e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            } finally {
                if (writeLock != null && writeCondition != null) {
                    writeLock.unlock();
                }
            }
        }
        return pass;
    }

    @Override
    public void manuallyDoTrigger() {
        synchronized (SimpleBufferTrigger.this) {
            doConsume();
        }
    }

    private void doConsume() {
        C old = null;
        try {
            if (writeLock != null) {
                writeLock.lock();
            }
            try {
                old = buffer.getAndSet(bufferFactory.get());
            } finally {
                counter.set(0);
                if (writeCondition != null) {
                    writeCondition.signalAll();
                }
                if (writeLock != null) {
                    writeLock.unlock();
                }
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

        TriggerResult canTrigger(long lastConsumeTimestamp, long changedCount);
    }

    public static class TriggerResult {

        private static final TriggerResult EMPTY = new TriggerResult(false, DAYS.toMillis(1));
        private final boolean doConsumer;
        private final long nextPeriod;

        private TriggerResult(boolean doConsumer, long nextPeriod) {
            this.doConsumer = doConsumer;
            this.nextPeriod = nextPeriod;
        }

        public static TriggerResult trig(boolean doConsumer, long nextPeriod) {
            return new TriggerResult(doConsumer, nextPeriod);
        }

        public static TriggerResult empty() {
            return EMPTY;
        }
    }

    private class TriggerRunnable implements Runnable {

        private final ScheduledExecutorService scheduledExecutorService;
        private final TriggerStrategy triggerStrategy;

        TriggerRunnable(ScheduledExecutorService scheduledExecutorService,
                TriggerStrategy triggerStrategy) {
            this.scheduledExecutorService = scheduledExecutorService;
            this.triggerStrategy = triggerStrategy;
        }

        @Override
        public void run() {
            synchronized (SimpleBufferTrigger.this) {
                long nextTrigPeriod = DEFAULT_NEXT_TRIGGER_PERIOD;
                try {
                    TriggerResult triggerResult = triggerStrategy.canTrigger(lastConsumeTimestamp,
                            counter.get());
                    nextTrigPeriod = triggerResult.nextPeriod;
                    long beforeConsume = currentTimeMillis();
                    if (triggerResult.doConsumer) {
                        lastConsumeTimestamp = beforeConsume;
                        doConsume();
                    }
                    nextTrigPeriod = nextTrigPeriod - (currentTimeMillis() - beforeConsume);
                } catch (Throwable e) {
                    logger.error("", e);
                }
                nextTrigPeriod = Math.max(0, nextTrigPeriod);
                scheduledExecutorService.schedule(this, nextTrigPeriod, MILLISECONDS);
            }
        }
    }
}
