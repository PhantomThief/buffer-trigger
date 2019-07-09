package com.github.phantomthief.test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.out;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.collection.impl.SimpleBufferTrigger;

/**
 * @author w.vela
 * Created on 09/08/2016.
 */
class LongConsumerTest {

    @Test
    void testShort() {
        AtomicInteger counter = new AtomicInteger();
        BufferTrigger<Long> bufferTrigger = SimpleBufferTrigger.newBuilder()
                .interval(1, SECONDS)
                .consumer(set -> {
                    sleepUninterruptibly(700, MILLISECONDS);
                    counter.incrementAndGet();
                })
                .build();
        long now = currentTimeMillis();
        while (currentTimeMillis() - now < SECONDS.toMillis(20)) {
            bufferTrigger.enqueue(1L);
            sleepUninterruptibly(10, MILLISECONDS);
        }
        int count = counter.get();
        out.println("short consumer count:" + count);
        assertTrue(count >= 19 && count <= 21);
    }

    @Test
    void testLong() {
        AtomicInteger counter = new AtomicInteger();
        BufferTrigger<Long> bufferTrigger = SimpleBufferTrigger.newBuilder()
                .interval(1, SECONDS)
                .consumer(set -> {
                    sleepUninterruptibly(2000, MILLISECONDS);
                    counter.incrementAndGet();
                })
                .build();
        long now = currentTimeMillis();
        while (currentTimeMillis() - now < SECONDS.toMillis(20)) {
            bufferTrigger.enqueue(1L);
            sleepUninterruptibly(10, MILLISECONDS);
        }
        int count = counter.get();
        out.println("long consumer count:" + count);
        assertTrue(count >= 9 && count <= 11);
    }
}
