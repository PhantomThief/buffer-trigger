package com.github.phantomthief.test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.collection.impl.MultiIntervalTriggerStrategy;
import com.github.phantomthief.collection.impl.SimpleBufferTrigger;
import com.google.common.collect.Interner;

/**
 * @author w.vela
 * Created on 15/07/2016.
 */
class MultiIntervalTriggerTest {

    @Test
    void test() {
        AtomicInteger assertSize = new AtomicInteger();
        BufferTrigger<Integer> bufferTrigger = SimpleBufferTrigger
                .<Integer, Set<Interner>> newGenericBuilder() //
                .triggerStrategy(new MultiIntervalTriggerStrategy() //
                        .on(10, SECONDS, 1) //
                        .on(5, SECONDS, 10) //
                        .on(1, SECONDS, 100) //
                ) //
                .consumer(set -> {
                    System.out.println("size:" + set.size());
                    assertEquals(set.size(), assertSize.get());
                }) //
                .build();

        enqueue(bufferTrigger, 100);
        assertSize.set(100);
        sleep(2);
        enqueue(bufferTrigger, 10);
        assertSize.set(10);
        sleep(6);
        enqueue(bufferTrigger, 1);
        assertSize.set(1);
        sleep(11);

        sleepUninterruptibly(10, SECONDS);
    }

    @Test
    void testInvalidBuild() {
        assertThrows(IllegalArgumentException.class,
                () -> SimpleBufferTrigger.<Integer, Set<Interner>> newGenericBuilder() //
                        .triggerStrategy(new MultiIntervalTriggerStrategy() //
                                .on(1, SECONDS, 1) //
                                .on(2, SECONDS, 2) //
                        ).consumer(set -> System.out.println("size:" + set.size())) //
                        .build());
    }

    private void enqueue(BufferTrigger<Integer> trigger, int size) {
        System.out.println("start enqueue " + size);
        for (int i = 0; i < size; i++) {
            trigger.enqueue(i);
        }
        System.out.println("start enqueue " + size);
    }

    private void sleep(int second) {
        System.out.println("start sleep " + second);
        sleepUninterruptibly(second, SECONDS);
        System.out.println("end sleep " + second);
    }
}
