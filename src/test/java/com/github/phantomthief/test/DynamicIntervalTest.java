package com.github.phantomthief.test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.collection.BufferTrigger;

/**
 * @author w.vela
 * Created on 2021-01-29.
 */
class DynamicIntervalTest {
    private static final Logger logger = LoggerFactory.getLogger(DynamicIntervalTest.class);

    @Test
    void test() {
        int[] interval = {2};
        AtomicInteger counter = new AtomicInteger();
        BufferTrigger<String> buffer = BufferTrigger.<String, Integer> simple()
                .setContainer(() -> 0, (_1, _2) -> true)
                .consumer(it -> {
                    logger.info("before consumer:{}", counter);
                    counter.incrementAndGet();
                    logger.info("after consumer:{}", counter);
                })
                .interval(() -> interval[0], SECONDS)
                .build();

        int current = counter.get();
        for (int i = 0; i < 5; i++) {
            buffer.enqueue("test");
            sleepUninterruptibly(interval[0], SECONDS);
            assertEquals(current++, counter.get());
        }
        interval[0] = 3;
        current = counter.get();
        for (int i = 0; i < 5; i++) {
            buffer.enqueue("test");
            sleepUninterruptibly(interval[0], SECONDS);
            assertEquals(current++, counter.get());
        }
    }
}
