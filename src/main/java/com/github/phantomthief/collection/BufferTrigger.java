package com.github.phantomthief.collection;

import com.github.phantomthief.collection.impl.BatchConsumeBlockingQueueTrigger;
import com.github.phantomthief.collection.impl.BatchConsumerTriggerBuilder;
import com.github.phantomthief.collection.impl.GenericBatchConsumerTriggerBuilder;
import com.github.phantomthief.collection.impl.GenericSimpleBufferTriggerBuilder;
import com.github.phantomthief.collection.impl.SimpleBufferTrigger;
import com.github.phantomthief.collection.impl.SimpleBufferTriggerBuilder;

/**
 * 一个支持自定义消费策略的本地缓存.
 * <p>用于本地缓存指定条目数据集，定时进行批处理或聚合计算；可用于埋点聚合计算，点赞计数等不追求强一致的业务场景.
 * <p>大多数场景推荐调用{@link #simple()}来构造{@link BufferTrigger}.
 *
 * @author w.vela
 */
public interface BufferTrigger<E> extends AutoCloseable {

    /**
     * 将需要定时处理的元素推入缓存.
     *
     * @throws IllegalStateException 当实例被关闭时，调用该方法可能会引起该异常.
     */
    void enqueue(E element);

    /**
     * 手动触发一次缓存消费.
     * <p>一般处于缓存关闭方法{@link #close()}实现中.
     */
    void manuallyDoTrigger();

    /**
     * 获取当前缓存中未消费元素个数.
     */
    long getPendingChanges();

    /**
     * 快捷创建{@link GenericSimpleBufferTriggerBuilder}建造器，用于构造{@link SimpleBufferTrigger}实例.
     * <p>
     * 推荐用该方法构造{@link SimpleBufferTrigger}实例，适用用大多数场景
     *
     * @param <E> 元素类型
     * @param <C> 持有元素的容器类型，如使用默认容器实现（默认实现将{@link java.util.concurrent.ConcurrentHashMap}
     * 包装为{@link java.util.Set})，建议传递<code>Set&lt;E&gt;</code>.
     * @return {@link GenericSimpleBufferTriggerBuilder} 实例
     */
    static <E, C> GenericSimpleBufferTriggerBuilder<E, C> simple() {
        return new GenericSimpleBufferTriggerBuilder<>(SimpleBufferTrigger.newBuilder());
    }

    /**
     * 该方法即将废弃，可更换为{@link #simple()}.
     */
    @Deprecated
    static SimpleBufferTriggerBuilder<Object, Object> simpleTrigger() {
        return SimpleBufferTrigger.newBuilder();
    }

    /**
     * 提供自带背压(back-pressure)的简单批量归并消费能力.
     * <p>
     *
     * <p>
     * FIXME: 高亮注意，目前 {@link #simple()} 也提供了背压能力 {@link GenericSimpleBufferTriggerBuilder#enableBackPressure()}
     * 所以本模式的意义已经小的多，如果没特殊情况，可以考虑都切换到 {@link SimpleBufferTrigger} 版本.
     */
    static <E> GenericBatchConsumerTriggerBuilder<E> batchBlocking() {
        return new GenericBatchConsumerTriggerBuilder<>(
                BatchConsumeBlockingQueueTrigger.newBuilder());
    }

    /**
     * 该方法即将废弃，请使用{@link #batchBlocking()}替代.
     */
    @Deprecated
    static BatchConsumerTriggerBuilder<Object> batchBlockingTrigger() {
        return BatchConsumeBlockingQueueTrigger.newBuilder();
    }

    /**
     * 停止该实例，释放所持有资源.
     * <p>
     * 请注意自动关闭特性仅在处于{@code try}-with-resources时才会生效，其它场景请在服务停止的回调方法中显式调用该方法.
     */
    @Override
    void close(); // override to remove throws Exception.
}
