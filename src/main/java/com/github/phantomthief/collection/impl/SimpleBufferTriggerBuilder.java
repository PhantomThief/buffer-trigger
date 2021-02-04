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

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.collection.impl.SimpleBufferTrigger.TriggerStrategy;
import com.github.phantomthief.util.ThrowableConsumer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * {@link SimpleBufferTrigger}构造器，目前已不推荐直接使用，请调用{@link BufferTrigger#simple()}生成构造器.
 * <p>
 * 用于标准化{@link SimpleBufferTrigger}实例生成及配置，提供部分机制的默认实现
 * @param <E> 缓存元素类型，标明{@link  SimpleBufferTrigger#enqueue(Object)}传入元素的类型
 * @param <C> 缓存容器类型
 */
@SuppressWarnings("unchecked")
public class SimpleBufferTriggerBuilder<E, C> {

    private static final Logger logger = LoggerFactory.getLogger(SimpleBufferTriggerBuilder.class);
    private static NameRegistry globalNameRegistry;

    private boolean maxBufferCountWasSet = false;

    TriggerStrategy triggerStrategy;
    ScheduledExecutorService scheduledExecutorService;
    boolean usingInnerExecutor;
    Supplier<C> bufferFactory;
    ToIntBiFunction<C, E> queueAdder;
    ThrowableConsumer<C, Throwable> consumer;
    BiConsumer<Throwable, C> exceptionHandler;
    LongSupplier maxBufferCount = () -> -1;
    RejectHandler<E> rejectHandler;
    String name;
    boolean disableSwitchLock;

    /**
     * 设置缓存提供器及缓存存入函数；<b>注意：</b> 请使用者必须考虑线程安全问题.
     * <p>
     * 为了保证不同场景下的最优性能，框架本身不保证{@link BufferTrigger#enqueue(Object)}操作的线程安全，
     * 一般该函数调用位于处理请求线程池，该函数的耗时将直接影响业务服务并发效率，
     * 推荐使用者实现更贴合实际业务的高性能线程安全容器.
     * <p>
     * 另，如确定{@link BufferTrigger#enqueue(Object)}为单线程调用，无需使用线程安全容器.
     * <p>
     * 推荐使用{@link #setContainerEx(Supplier, ToIntBiFunction)}，
     * 对于需要传入的queueAdder类型为{@link ToIntBiFunction}，更符合计数行为需求.
     * @param factory 缓存提供器；要求为{@link Supplier}，因为执行消费回调后，缓存会被清空，需要执行提供器再次获得可用缓存对象
     * @param queueAdder 缓存更新方法，要求返回元素插入结果（布尔值），如缓存容器实现为{@link java.util.HashSet},
     * 在插入相同元素时会返回false，类库需要获得该结果转化为缓存中变动元素个数并计数，该计数可能作为消费行为的触发条件.
     * @param <E1> 缓存元素类型
     * @param <C1> 缓存容器类型
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
     * 设置缓存提供器及缓存存入函数；<b>注意：</b> 必须使用线程安全容器.
     * <p>
     * 为了保证不同场景下的最优性能，框架本身不保证{@link BufferTrigger#enqueue(Object)}操作的线程安全，
     * 一般函数调用位于处理请求线程池，该函数的耗时将直接影响业务服务并发效率，
     * 推荐使用者实现更贴合实际业务的线程安全容器.
     * <p>
     * 另，如确定{@link BufferTrigger#enqueue(Object)}为单线程调用，无需使用线程安全容器.
     * @param factory 缓存提供器；要求为{@link Supplier}，因为执行消费回调后，缓存会被清空，需要执行提供器再次获得可用缓存对象
     * @param queueAdder 缓存更新方法，要求返回元素变动个数，类库需要将结果计数，该计数可能作为消费行为的触发条件.
     * @param <E1> 缓存元素类型
     * @param <C1> 缓存容器类型
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

    /**
     * 自定义计划任务线程池，推荐使用内部默认实现.
     * <p>
     * 如使用自定义线程池，在调用{@link BufferTrigger#close()}方法后需要手动调用{@link ScheduledExecutorService#shutdown()}
     * 以及{@link ScheduledExecutorService#awaitTermination(long, TimeUnit)}方法来保证线程池平滑停止.
     */
    public SimpleBufferTriggerBuilder<E, C>
            setScheduleExecutorService(ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
        return this;
    }

    /**
     * 设置异常处理器.
     * <p>
     * 该处理器会在消费异常时执行.
     */
    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1>
            setExceptionHandler(BiConsumer<? super Throwable, ? super C1> exceptionHandler) {
        SimpleBufferTriggerBuilder<E1, C1> thisBuilder = (SimpleBufferTriggerBuilder<E1, C1>) this;
        thisBuilder.exceptionHandler = (BiConsumer<Throwable, C1>) exceptionHandler;
        return thisBuilder;
    }

    /**
     * 是否关闭读写锁，默认读写锁为开启状态，不推荐关闭.
     * <p>
     * {@link BufferTrigger}使用场景可归纳为读多写少，{@link SimpleBufferTrigger}的实现中默认使用了
     * 可重入读写锁{@link java.util.concurrent.locks.ReentrantReadWriteLock}
     */
    public SimpleBufferTriggerBuilder<E, C> disableSwitchLock() {
        this.disableSwitchLock = true;
        return this;
    }

    /**
     * 设置消费触发策略.
     * @param triggerStrategy {@link TriggerStrategy}具体实现，
     * 推荐传入{@link MultiIntervalTriggerStrategy}实例
     */
    public SimpleBufferTriggerBuilder<E, C> triggerStrategy(TriggerStrategy triggerStrategy) {
        this.triggerStrategy = triggerStrategy;
        return this;
    }

    /**
     * 该方法即将废弃，请使用{@link #interval(long, TimeUnit)}或{@link #triggerStrategy}设置消费策略.
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

    /**
     * 设置定期触发的简单消费策略，{@link #interval(LongSupplier, TimeUnit)}易用封装
     * <p>
     * 该方法间隔时间在初始化时设定，无法更改，如有动态间隔时间需求，请使用{@link #interval(LongSupplier, TimeUnit)}
     * @param interval 间隔时间
     * @param unit 间隔时间单位
     */
    public SimpleBufferTriggerBuilder<E, C> interval(long interval, TimeUnit unit) {
        return interval(() -> interval, unit);
    }

    /**
     * 设置定期触发的简单消费策略.
     * <p>
     * 该方法提供了动态更新时间间隔的需求，如时间间隔设置在配置中心或redis中，会因场景动态变化.
     * <p>
     * 如果需同时存在多个不同周期的消费策略，请使用{@link MultiIntervalTriggerStrategy}
     * @param interval 间隔时间提供器，可自行实现动态时间
     * @param unit 间隔时间单位
     */
    public SimpleBufferTriggerBuilder<E, C> interval(LongSupplier interval, TimeUnit unit) {
        this.triggerStrategy = (last, change) -> {
            long intervalInMs = unit.toMillis(interval.getAsLong());
            return trig(change > 0 && currentTimeMillis() - last >= intervalInMs, intervalInMs);
        };
        return this;
    }

    /**
     * 设置消费回调函数.
     * <p>
     * 该方法用于设定消费行为，由使用者自行提供消费回调函数，需要注意，
     * 回调注入的对象为当前消费时间点，缓存容器中尚存的所有元素，非逐个元素消费.
     * @param consumer 
     * @param <E1> 缓存元素类型
     * @param <C1> 缓存容器类型
     */
    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1>
            consumer(ThrowableConsumer<? super C1, Throwable> consumer) {
        checkNotNull(consumer);
        SimpleBufferTriggerBuilder<E1, C1> thisBuilder = (SimpleBufferTriggerBuilder<E1, C1>) this;
        thisBuilder.consumer = (ThrowableConsumer<C1, Throwable>) consumer;
        return thisBuilder;
    }

    /**
     * 设置缓存最大容量（数量）.
     * <p>
     * 推荐由缓存容器实现该限制
     */
    public SimpleBufferTriggerBuilder<E, C> maxBufferCount(long count) {
        checkArgument(count > 0);
        return maxBufferCount(() -> count);
    }

    /**
     * 设置缓存最大容量（数量）提供器，适合动态场景.
     * <p>
     * 推荐由缓存容器实现该限制
     */
    public SimpleBufferTriggerBuilder<E, C> maxBufferCount(@Nonnull LongSupplier count) {
        this.maxBufferCount = checkNotNull(count);
        maxBufferCountWasSet = true;
        return this;
    }

    /**
     * 同时设置缓存最大容量（数量）以及拒绝推入处理器
     */
    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1> maxBufferCount(long count,
            Consumer<? super E1> rejectHandler) {
        return (SimpleBufferTriggerBuilder<E1, C1>) maxBufferCount(count)
                .rejectHandler(rejectHandler);
    }

    /**
     * 设置拒绝推入缓存处理器.
     * <p>
     * 当设置maxBufferCount，且达到上限时回调该处理器；推荐由缓存容器实现该限制.
     */
    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1> rejectHandler(Consumer<? super E1> rejectHandler) {
        checkNotNull(rejectHandler);
        return this.rejectHandlerEx((e, h) -> {
            rejectHandler.accept(e);
            return false;
        });
    }

    /**
     * 开启背压(back-pressure)能力，无处理回调.
     * <p>
     * <b>注意，当开启背压时，需要配合 {@link #maxBufferCount(long)}
     * 并且不要设置 {@link #rejectHandler}.</b>
     * <p>
     * 当buffer达到最大值时，会阻塞{@link BufferTrigger#enqueue(Object)}调用，直到消费完当前buffer后再继续执行；
     * 由于enqueue大多处于处理请求线程池中，如开启背压，大概率会造成请求线程池耗尽，此类场景建议直接丢弃入队元素或转发至Kafka.
     */
    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1> enableBackPressure() {
        return enableBackPressure(null);
    }

    /**
     * 开启背压(back-pressure)能力，接收处理回调方法.
     * <p>
     * <b>注意，当开启背压时，需要配合 {@link #maxBufferCount(long)}
     * 并且不要设置 {@link #rejectHandler}.</b>
     * <p>
     * 当buffer达到最大值时，会阻塞{@link BufferTrigger#enqueue(Object)}调用，直到消费完当前buffer后再继续执行；
     * 由于enqueue大多处于处理请求线程池中，如开启背压，大概率会造成请求线程池耗尽，此类场景建议直接丢弃入队元素或转发至Kafka.
     */
    public <E1, C1> SimpleBufferTriggerBuilder<E1, C1> enableBackPressure(BackPressureListener<E1> listener) {
        if (this.rejectHandler != null) {
            throw new IllegalStateException("cannot enable back-pressure while reject handler was set.");
        }
        SimpleBufferTriggerBuilder<E1, C1> thisBuilder = (SimpleBufferTriggerBuilder<E1, C1>) this;
        thisBuilder.rejectHandler = new BackPressureHandler<>(listener);
        return thisBuilder;
    }

    /**
     * it's better dealing this in container
     */
    private <E1, C1> SimpleBufferTriggerBuilder<E1, C1> rejectHandlerEx(RejectHandler<? super E1> rejectHandler) {
        checkNotNull(rejectHandler);
        if (this.rejectHandler instanceof BackPressureHandler) {
            throw new IllegalStateException("cannot set reject handler while enable back-pressure.");
        }
        SimpleBufferTriggerBuilder<E1, C1> thisBuilder = (SimpleBufferTriggerBuilder<E1, C1>) this;
        thisBuilder.rejectHandler = (RejectHandler<E1>) rejectHandler;
        return thisBuilder;
    }

    /**
     * 设置计划任务线程名称.
     * <p>
     * 仅当使用默认计划任务线程池时生效，线程最终名称为pool-simple-buffer-trigger-thread-[name].
     */
    public SimpleBufferTriggerBuilder<E, C> name(String name) {
        this.name = name;
        return this;
    }

    /**
     * 生成实例
     */
    public <E1> BufferTrigger<E1> build() {
        check();
        if (globalNameRegistry != null && name == null) {
            name = globalNameRegistry.name();
        }
        return new LazyBufferTrigger<>(() -> {
            ensure();
            SimpleBufferTriggerBuilder<E1, C> builder =
                    (SimpleBufferTriggerBuilder<E1, C>) SimpleBufferTriggerBuilder.this;
            return new SimpleBufferTrigger<>(builder);
        });
    }

    private void check() {
        checkNotNull(consumer);
        if (rejectHandler instanceof BackPressureHandler) {
            if (disableSwitchLock) {
                throw new IllegalStateException("back-pressure cannot work together with switch lock disabled.");
            }
            if (!maxBufferCountWasSet) {
                throw new IllegalStateException("back-pressure need to set maxBufferCount.");
            }
        }
    }

    private void ensure() {
        if (triggerStrategy == null) {
            logger.warn("no trigger strategy found. using NO-OP trigger");
            triggerStrategy = (t, n) -> empty();
        }

        if (bufferFactory == null && queueAdder == null) {
            logger.warn("no container found. use default thread-safe HashSet as container.");
            bufferFactory = () -> (C) newSetFromMap(new ConcurrentHashMap<>());
            queueAdder = (c, e) -> ((Set<E>) c).add(e) ? 1 : 0;
        }
        if (scheduledExecutorService == null) {
            scheduledExecutorService = makeScheduleExecutor();
            usingInnerExecutor = true;
        }
        if (name != null && rejectHandler instanceof BackPressureHandler) {
            ((BackPressureHandler<E>) rejectHandler).setName(name);
        }
    }

    private ScheduledExecutorService makeScheduleExecutor() {
        String threadPattern = name == null ? "pool-simple-buffer-trigger-thread-%d" : "pool-simple-buffer-trigger-thread-["
                + name + "]";
        return newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat(threadPattern)
                        .setDaemon(true)
                        .build());
    }

    static void setupGlobalNameRegistry(NameRegistry registry) {
        globalNameRegistry = registry;
    }
}