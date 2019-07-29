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
