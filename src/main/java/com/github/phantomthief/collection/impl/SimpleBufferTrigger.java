package com.github.phantomthief.collection.impl;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
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
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToIntBiFunction;

import org.slf4j.Logger;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.util.ThrowableConsumer;

/**
 * {@link BufferTrigger}的通用实现，适合大多数业务场景
 * <p>
 * 消费触发策略会考虑消费回调函数的执行时间，实际执行间隔 = 理论执行间隔 - 消费回调函数执行时间；
 * 如回调函数执行时间已超过理论执行间隔，将立即执行下一次消费任务.
 *
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
    private final LongSupplier maxBufferCount;
    private final RejectHandler<E> rejectHandler;
    private final ReadLock readLock;
    private final WriteLock writeLock;
    private final Condition writeCondition;
    private final Runnable shutdownExecutor;

    private volatile boolean shutdown;
    private volatile long lastConsumeTimestamp = currentTimeMillis();

    /**
     * 使用提供的构造器创建SimpleBufferTrigger实例
     */
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
        this.shutdownExecutor = () -> {
            if (builder.usingInnerExecutor) {
                shutdownAndAwaitTermination(builder.scheduledExecutorService, 1, DAYS);
            }
        };
    }

    /**
     * 该方法即将废弃，可更换为{@link BufferTrigger#simple()}创建适合大多数场景的通用实例.
     */
    @Deprecated
    public static SimpleBufferTriggerBuilder<Object, Object> newBuilder() {
        return new SimpleBufferTriggerBuilder<>();
    }

    /**
     * 该方法即将废弃，可更换为{@link BufferTrigger#simple()}创建适合大多数场景的通用实例.
     */
    @Deprecated
    public static <E, C> GenericSimpleBufferTriggerBuilder<E, C> newGenericBuilder() {
        return new GenericSimpleBufferTriggerBuilder<>(newBuilder());
    }

    /**
     * 快捷创建附带计数器容器的{@link SimpleBufferTriggerBuilder}实例.
     * <p>
     * 目前不推荐使用
     */
    public static SimpleBufferTriggerBuilder<Object, Map<Object, Integer>> newCounterBuilder() {
        return new SimpleBufferTriggerBuilder<Object, Map<Object, Integer>>()
                .setContainer(ConcurrentHashMap::new, (map, element) -> {
                    map.merge(element, 1, Math::addExact);
                    return true;
                });
    }

    /**
     * 将需要定时处理的元素推入缓存.
     *
     * <p>存在缓存容量检测，如果缓存已满，
     * 且拒绝回调方法已通过{@link SimpleBufferTriggerBuilder#rejectHandler(Consumer)}设置，
     * 则会回调该方法.
     *
     * <p>需要特别注意，该实现为了避免加锁引起的性能损耗，以及考虑到应用场景，缓存容量检测不进行严格计数，
     * 在并发场景下缓存中实际元素数量会略微超过预设的最大缓存容量；
     * 如需严格计数场景，请自行实现{@link BufferTrigger}.
     *
     * @param element 符合声明参数类型的元素
     */
    @Override
    public void enqueue(E element) {
        checkState(!shutdown, "buffer trigger was shutdown.");

        long currentCount = counter.get();
        long thisMaxBufferCount = maxBufferCount.getAsLong();
        if (thisMaxBufferCount > 0 && currentCount >= thisMaxBufferCount) {
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

    /**
     * 停止该实例，执行线程池shutdown操作.
     * <p>
     * 在停止实例前，会强制触发一次消费，用于清空缓存中尚未消费的元素.
     * <p>
     * 如未通过{@link GenericSimpleBufferTriggerBuilder#setScheduleExecutorService(ScheduledExecutorService)}
     * 自定义计划任务线程池，会调用默认计划任务线程池shutdown()方法，如未停止成功，
     * 会保留1天的超时时间再强制停止线程池.
     */
    @Override
    public void close() {
        shutdown = true;
        try {
            manuallyDoTrigger();
        } finally {
            shutdownExecutor.run();
        }
    }

    /**
     * 触发消费策略接口.
     * <p>
     * 如需自定义消费策略，请参考{@link MultiIntervalTriggerStrategy}代码
     */
    public interface TriggerStrategy {

        /**
         * 获取触发器执行结果，用于判断是否可执行消费回调
         */
        TriggerResult canTrigger(long lastConsumeTimestamp, long changedCount);
    }

    /**
     * 触发器执行结果，计划任务内部使用，一般无需关注
     */
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
                if (!shutdown) {
                    scheduledExecutorService.schedule(this, nextTrigPeriod, MILLISECONDS);
                }
            }
        }
    }
}
