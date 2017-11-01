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

    LazyBufferTrigger(Supplier<BufferTrigger<E>> factory) {
        this.factory = lazy(factory);
    }

    @Override
    public void enqueue(E element) {
        this.factory.get().enqueue(element);
    }

    @Override
    public void manuallyDoTrigger() {
        this.factory.ifPresent(BufferTrigger::manuallyDoTrigger);
    }

    @Override
    public long getPendingChanges() {
        return this.factory.map(BufferTrigger::getPendingChanges).orElse(0L);
    }
}
