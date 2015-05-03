/**
 * 
 */
package com.github.phantomthief.collection.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.trigger.Trigger;
import com.github.phantomthief.trigger.impl.ScheduledTrigger;
import com.github.phantomthief.trigger.impl.ScheduledTrigger.TriggerBuilder;

/**
 * @author w.vela
 */
public class BaseBufferTrigger<E> implements BufferTrigger<E> {

    private static Logger logger = LoggerFactory.getLogger(BaseBufferTrigger.class);

    private final AtomicReference<Object> buffer;
    private final Supplier<Object> bufferFactory;
    private final Trigger trigger;
    private final BiPredicate<Object, E> queueAdder;

    private BaseBufferTrigger(AtomicReference<Object> buffer, Supplier<Object> bufferFactory,
            Trigger trigger, BiPredicate<Object, E> queueAdder) {
        this.buffer = buffer;
        this.bufferFactory = bufferFactory;
        this.trigger = trigger;
        this.queueAdder = queueAdder;
    }

    @Override
    public void enqueue(E element) {
        Object thisBuffer = buffer.updateAndGet(old -> old != null ? old : bufferFactory.get());
        boolean addSuccess = queueAdder.test(thisBuffer, element);
        if (addSuccess) {
            trigger.markChange();
        }
    }

    @Override
    public void manuallyDoTrigger() {
        trigger.doAction();
    }

    public static final <E, C> Builder<E, C> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<E, C> {

        private final TriggerBuilder triggerBuilder = new TriggerBuilder();
        private final AtomicReference<C> buffer = new AtomicReference<>();
        private Supplier<C> bufferFactory;
        private BiPredicate<C, E> queueAdder;

        public Builder<E, C> setContainer(Supplier<C> factory, BiPredicate<C, E> queueAdder) {
            if (factory == null || queueAdder == null) {
                throw new IllegalArgumentException();
            }
            this.bufferFactory = factory;
            this.queueAdder = queueAdder;
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

        @SuppressWarnings("unchecked")
        public BaseBufferTrigger<E> build() {
            ScheduledTrigger trigger = triggerBuilder.build();
            ensure();
            return new BaseBufferTrigger<E>((AtomicReference<Object>) buffer,
                    (Supplier<Object>) bufferFactory, trigger, (BiPredicate<Object, E>) queueAdder);
        }

        @SuppressWarnings("unchecked")
        private void ensure() {
            if (bufferFactory == null) {
                bufferFactory = () -> (C) Collections.synchronizedSet(new HashSet<>());
            }
            if (queueAdder == null) {
                queueAdder = (c, e) -> ((Set<E>) c).add(e);
            }
        }

        @SuppressWarnings("unchecked")
        private Runnable makeFunction(Consumer<C> consumer) {
            return () -> {
                try {
                    Object old = buffer.getAndSet(bufferFactory.get());
                    consumer.accept((C) old);
                } catch (Throwable e) {
                    logger.error("Ops.", e);
                }
            };
        }

    }

}
