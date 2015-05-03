/**
 * 
 */
package com.github.phantomthief.collection.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.trigger.Trigger;
import com.github.phantomthief.trigger.impl.ScheduledTrigger;
import com.github.phantomthief.trigger.impl.ScheduledTrigger.TriggerBuilder;

/**
 * @author w.vela
 */
public class CollectionBufferTrigger<E, C extends Collection<E>> implements BufferTrigger<E> {

    private final AtomicReference<C> buffer;
    private final Supplier<C> bufferFactory;
    private final Trigger trigger;

    private CollectionBufferTrigger(AtomicReference<C> buffer, Supplier<C> bufferFactory,
            Trigger trigger) {
        this.buffer = buffer;
        this.bufferFactory = bufferFactory;
        this.trigger = trigger;
    }

    @Override
    public void enqueue(E element) {
        C thisBuffer = buffer.updateAndGet(old -> old != null ? old : bufferFactory.get());
        boolean addSuccess = thisBuffer.add(element);
        if (addSuccess) {
            trigger.markChange();
        }
    }

    @Override
    public void manuallyDoTrigger() {
        trigger.doAction();
    }

    public static final <E, C extends Collection<E>> Builder<E, C> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<E, C extends Collection<E>> {

        private final TriggerBuilder triggerBuilder = new TriggerBuilder();
        private final AtomicReference<C> buffer = new AtomicReference<>();
        private Supplier<C> bufferFactory;

        public Builder<E, C> setBufferFactory(Supplier<C> factory) {
            this.bufferFactory = factory;
            return this;
        }

        public Builder<E, C> on(long interval, TimeUnit unit, long count, Consumer<C> consumer) {
            triggerBuilder.on(interval, unit, count, makeFunction(consumer));
            return this;
        }

        public Builder<E, C> fixedRate(long interval, TimeUnit unit, Consumer<C> consumer) {
            triggerBuilder.fixedRate(interval, unit, makeFunction(consumer));
            return this;
        }

        public CollectionBufferTrigger<E, C> build() {
            ScheduledTrigger trigger = triggerBuilder.build();
            ensure();
            return new CollectionBufferTrigger<>(buffer, bufferFactory, trigger);
        }

        @SuppressWarnings("unchecked")
        private void ensure() {
            if (bufferFactory == null) {
                bufferFactory = () -> (C) Collections.synchronizedSet(new HashSet<>());
            }
        }

        private Runnable makeFunction(Consumer<C> consumer) {
            return () -> {
                C old = buffer.getAndSet(bufferFactory.get());
                consumer.accept(old);
            };
        }

    }

}
