package com.github.phantomthief.collection.impl;

import static com.github.phantomthief.collection.impl.SimpleBufferTrigger.TriggerResult.trig;
import static com.github.phantomthief.util.MoreSuppliers.lazy;
import static java.lang.System.currentTimeMillis;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToIntBiFunction;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.collection.impl.SimpleBufferTrigger.TriggerResult;
import com.github.phantomthief.collection.impl.SimpleBufferTrigger.TriggerStrategy;
import com.github.phantomthief.util.ThrowableConsumer;

/**
 * {@link SimpleBufferTrigger}通用构造器
 * <p>
 * 当对{@link SimpleBufferTrigger}有更多自定义配置时可使用该构造器.
 * <p>
 * 本质上包装了{@link SimpleBufferTriggerBuilder}，屏蔽底层细节.
 * @param <E> 缓存元素类型，标明{@link  SimpleBufferTrigger#enqueue(Object)}传入元素的类型
 * @param <C> 缓存容器类型
 * @author w.vela
 */
public class GenericSimpleBufferTriggerBuilder<E, C> {

    private final SimpleBufferTriggerBuilder<Object, Object> builder;

    /**
     * 构造方法，接受一个{@link SimpleBufferTriggerBuilder}进行包装.
     * <p>
     * 大多数业务场景，请使用{@link BufferTrigger#simple()}生成构造器实例.
     * @param builder
     */
    public GenericSimpleBufferTriggerBuilder(SimpleBufferTriggerBuilder<Object, Object> builder) {
        this.builder = builder;
    }

    /**
     * 设置缓存提供器及缓存存入函数；<b>注意：</b> 请使用者必须考虑线程安全问题.
     * <p>
     * 为了保证不同场景下的最优性能，框架本身不保证{@link BufferTrigger#enqueue(Object)}操作的线程安全，
     * 一般函数调用位于处理请求线程池，该函数的耗时将直接影响业务服务并发效率，
     * 推荐使用者实现更贴合实际业务的高性能线程安全容器.
     * <p>
     * 另，如确定{@link BufferTrigger#enqueue(Object)}为单线程调用，无需使用线程安全容器.
     * <p>
     * 推荐使用{@link #setContainerEx(Supplier, ToIntBiFunction)}，
     * 对于需要传入的queueAdder类型为{@link ToIntBiFunction}，更符合计数行为需求.
     * @param factory 缓存提供器；要求为{@link Supplier}，因为执行消费回调后，缓存会被清空，需要执行提供器再次获得可用缓存对象
     * @param queueAdder 缓存更新方法，要求返回元素插入结果（布尔值），如缓存容器实现为{@link java.util.HashSet},
     * 在插入相同元素时会返回false，类库需要获得该结果转化为缓存中变动元素个数并计数，该计数可能作为消费行为的触发条件.
     */
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> setContainer(Supplier<? extends C> factory,
            BiPredicate<? super C, ? super E> queueAdder) {
        builder.setContainer(factory, queueAdder);
        return this;
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
     */
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> setContainerEx(Supplier<? extends C> factory,
            ToIntBiFunction<? super C, ? super E> queueAdder) {
        builder.setContainerEx(factory, queueAdder);
        return this;
    }

    /**
     * 自定义计划任务线程池，推荐使用内部默认实现.
     * <p>
     * 如使用自定义线程池，在调用{@link BufferTrigger#close()}方法后需要手动调用{@link ScheduledExecutorService#shutdown()}
     * 以及{@link ScheduledExecutorService#awaitTermination(long, TimeUnit)}方法来保证线程池平滑停止.
     * @param scheduledExecutorService 线程池实例
     */
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C>
            setScheduleExecutorService(ScheduledExecutorService scheduledExecutorService) {
        builder.setScheduleExecutorService(scheduledExecutorService);
        return this;
    }

    /**
     * 设置异常处理器.
     * <p>
     * 该处理器会在消费异常时执行.
     */
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C>
            setExceptionHandler(BiConsumer<? super Throwable, ? super C> exceptionHandler) {
        builder.setExceptionHandler(exceptionHandler);
        return this;
    }

    /**
     * 设置消费触发策略.
     * @param triggerStrategy {@link TriggerStrategy}具体实现，
     * 推荐传入{@link MultiIntervalTriggerStrategy}实例
     */
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C>
            triggerStrategy(TriggerStrategy triggerStrategy) {
        builder.triggerStrategy(triggerStrategy);
        return this;
    }

    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> intervalAtFixedRate(long interval,
            TimeUnit unit) {
        builder.triggerStrategy(new TriggerStrategy() {

            private final Supplier<Long> time = lazy(System::currentTimeMillis);

            /**
             * always align to the first trig time
             */
            @Override
            public TriggerResult canTrigger(long last, long change) {
                long alignTime = time.get();
                long intervalInMs = unit.toMillis(interval);
                long result = intervalInMs - (currentTimeMillis() - alignTime) % intervalInMs;
                return trig(change > 0, result);
            }
        });
        return this;
    }

    /**
     * 该方法即将废弃，请使用{@link #interval(long, TimeUnit)}或{@link #triggerStrategy}设置消费策略.
     */
    @Deprecated
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> on(long interval, TimeUnit unit, long count) {
        builder.on(interval, unit, count);
        return this;
    }

    /**
     * 设置定期触发的简单消费策略，{@link #interval(LongSupplier, TimeUnit)}易用封装
     * <p>
     * 该方法间隔时间在初始化时设定，无法更改，如有动态间隔时间需求，请使用{@link #interval(LongSupplier, TimeUnit)}
     * @param interval 间隔时间
     * @param unit 间隔时间单位
     */
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> interval(long interval, TimeUnit unit) {
        builder.interval(interval, unit);
        return this;
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
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> interval(LongSupplier interval, TimeUnit unit) {
        builder.interval(interval, unit);
        return this;
    }

    /**
     * 设置消费回调函数.
     * <p>
     * 该方法用于设定消费行为，由使用者自行提供消费回调函数，需要注意，
     * 回调注入的对象为当前消费时间点，缓存容器中尚存的所有元素，非逐个元素消费.
     */
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C>
            consumer(ThrowableConsumer<? super C, Throwable> consumer) {
        builder.consumer(consumer);
        return this;
    }

    /**
     * 设置缓存最大容量（数量）.
     * <p>
     * 推荐由缓存容器实现该限制
     */
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> maxBufferCount(long count) {
        builder.maxBufferCount(count);
        return this;
    }

    /**
     * 设置缓存最大容量（数量）提供器，适合动态场景.
     * <p>
     * 推荐由缓存容器实现该限制
     */
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> maxBufferCount(@Nonnull LongSupplier count) {
        builder.maxBufferCount(count);
        return this;
    }

    /**
     * 关闭读写锁来提升性能，默认读写锁为开启状态，关闭后会引起推入缓存和消费逻辑的冲突，不推荐关闭.
     * <p>
     * 警告：获取锁重试次数最大为65535，请注意捕获处理异常.
     */
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> disableSwitchLock() {
        builder.disableSwitchLock();
        return this;
    }

    /**
     * 同时设置缓存最大容量（数量）以及拒绝推入处理器
     */
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> maxBufferCount(long count,
            Consumer<? super E> rejectHandler) {
        builder.maxBufferCount(count, rejectHandler);
        return this;
    }

    /**
     * 设置拒绝推入缓存处理器.
     * <p>
     * 当设置maxBufferCount，且达到上限时回调该处理器；推荐由缓存容器实现该限制.
     */
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> rejectHandler(Consumer<? super E> rejectHandler) {
        builder.rejectHandler(rejectHandler);
        return this;
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
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> enableBackPressure() {
        builder.enableBackPressure();
        return this;
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
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> enableBackPressure(BackPressureListener<E> listener) {
        builder.enableBackPressure(listener);
        return this;
    }

    /**
     * 设置计划任务线程名称.
     * <p>
     * 仅当使用默认计划任务线程池时生效，线程最终名称为pool-simple-buffer-trigger-thread-[name].
     */
    @CheckReturnValue
    public GenericSimpleBufferTriggerBuilder<E, C> name(String name) {
        builder.name(name);
        return this;
    }

    /**
     * 生成实例
     */
    public BufferTrigger<E> build() {
        return builder.build();
    }
}
