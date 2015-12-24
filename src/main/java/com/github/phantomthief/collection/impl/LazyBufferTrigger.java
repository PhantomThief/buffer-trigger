/**
 * 
 */
package com.github.phantomthief.collection.impl;

import static com.github.phantomthief.util.MoreSuppliers.lazy;

import java.util.function.Supplier;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.util.MoreSuppliers.CloseableSupplier;

/**
 * @author w.vela
 */
public class LazyBufferTrigger<E> implements BufferTrigger<E> {

    private final CloseableSupplier<BufferTrigger<E>> factory;

    /**
     * @param factory
     */
    LazyBufferTrigger(Supplier<BufferTrigger<E>> factory) {
        this.factory = lazy(factory);
    }

    /* (non-Javadoc)
     * @see com.github.phantomthief.collection.BufferTrigger#enqueue(java.lang.Object)
     */
    @Override
    public void enqueue(E element) {
        this.factory.get().enqueue(element);
    }

    /* (non-Javadoc)
     * @see com.github.phantomthief.collection.BufferTrigger#manuallyDoTrigger()
     */
    @Override
    public void manuallyDoTrigger() {
        this.factory.ifPresent(BufferTrigger::manuallyDoTrigger);
    }

    /* (non-Javadoc)
     * @see com.github.phantomthief.collection.BufferTrigger#getPendingChanges()
     */
    @Override
    public long getPendingChanges() {
        return this.factory.map(BufferTrigger::getPendingChanges).orElse(0L);
    }
}
