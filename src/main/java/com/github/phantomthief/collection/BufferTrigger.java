package com.github.phantomthief.collection;

import com.github.phantomthief.collection.impl.BatchConsumeBlockingQueueTrigger;
import com.github.phantomthief.collection.impl.BatchConsumerTriggerBuilder;
import com.github.phantomthief.collection.impl.GenericBatchConsumerTriggerBuilder;
import com.github.phantomthief.collection.impl.GenericSimpleBufferTriggerBuilder;
import com.github.phantomthief.collection.impl.SimpleBufferTrigger;
import com.github.phantomthief.collection.impl.SimpleBufferTriggerBuilder;

/**
 * @author w.vela
 */
public interface BufferTrigger<E> {

    void enqueue(E element);

    void manuallyDoTrigger();

    long getPendingChanges();

    static <E, C> GenericSimpleBufferTriggerBuilder<E, C> simple() {
        return new GenericSimpleBufferTriggerBuilder<>(SimpleBufferTrigger.newBuilder());
    }

    /**
     * use {@link #simple()} for better generic type support.
     */
    @Deprecated
    static SimpleBufferTriggerBuilder<Object, Object> simpleTrigger() {
        return SimpleBufferTrigger.newBuilder();
    }

    /**
     * 提供自带背压(back-pressure)的简单批量归并消费能力
     *
     * FIXME: 高亮注意，目前 {@link #simple()} 也提供了背压能力 {@link GenericSimpleBufferTriggerBuilder#enableBackPressure()}
     * 所以本模式的意义已经小的多，如果没特殊情况，可以考虑都且到 {@link SimpleBufferTrigger} 版本
     */
    static <E> GenericBatchConsumerTriggerBuilder<E> batchBlocking() {
        return new GenericBatchConsumerTriggerBuilder<>(
                BatchConsumeBlockingQueueTrigger.newBuilder());
    }

    /**
     * use {@link #batchBlocking()} for better generic type support.
     */
    @Deprecated
    static BatchConsumerTriggerBuilder<Object> batchBlockingTrigger() {
        return BatchConsumeBlockingQueueTrigger.newBuilder();
    }
}
