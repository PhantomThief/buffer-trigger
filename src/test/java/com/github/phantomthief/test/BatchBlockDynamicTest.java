package com.github.phantomthief.test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.collection.BufferTrigger;

/**
 * @author w.vela
 * Created on 2019-12-03.
 */
class BatchBlockDynamicTest {

    @Test
    void test() {
        Duration[] linger = {ofSeconds(1)};
        int[] consumed = {0};
        BufferTrigger<String> buffer = BufferTrigger.<String> batchBlocking()
                .batchSize(100)
                .linger(() -> linger[0])
                .setConsumerEx(it -> consumed[0]++)
                .build();
        for (int i = 0; i < 100; i++) {
            buffer.enqueue("test");
        }
        sleepUninterruptibly(20, MILLISECONDS);
        assertEquals(1, consumed[0]);
        for (int i = 0; i < 10; i++) {
            buffer.enqueue("test");
        }
        sleepUninterruptibly(20, MILLISECONDS);
        assertEquals(1, consumed[0]);
        sleepUninterruptibly(1, SECONDS);
        assertEquals(2, consumed[0]);

        linger[0] = ofSeconds(2);
        sleepUninterruptibly(1, SECONDS);
        for (int i = 0; i < 10; i++) {
            buffer.enqueue("test");
        }
        sleepUninterruptibly(1, SECONDS);
        assertEquals(2, consumed[0]);
        sleepUninterruptibly(1, SECONDS);
        assertEquals(3, consumed[0]);
    }
}
