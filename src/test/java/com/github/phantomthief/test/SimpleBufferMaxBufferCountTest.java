package com.github.phantomthief.test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.collection.BufferTrigger;

/**
 * @author w.vela
 * Created on 2019-12-03.
 */
class SimpleBufferMaxBufferCountTest {

    @Test
    void test() {
        long[] maxSize = {100};
        long[] sleep = {100000};
        long[] rejected = {0};
        BufferTrigger<String> trigger = BufferTrigger.<String, int[]> simple()
                .setContainer(() -> new int[] {0}, (it, e) -> {
                    it[0]++;
                    return true;
                })
                .maxBufferCount(() -> maxSize[0])
                .rejectHandler(it -> rejected[0]++)
                .consumer(it -> sleepUninterruptibly(sleep[0], MILLISECONDS))
                .build();
        for (int i = 0; i < 100; i++) {
            trigger.enqueue("test");
        }
        assertEquals(0, rejected[0]);
        trigger.enqueue("test");
        assertEquals(1, rejected[0]);
        maxSize[0] = 105;
        for (int i = 0; i < 5; i++) {
            trigger.enqueue("test");
        }
        assertEquals(1, rejected[0]);
        trigger.enqueue("test");
        assertEquals(2, rejected[0]);
        maxSize[0] = 110;
        for (int i = 0; i < 3; i++) {
            trigger.enqueue("test");
        }
        assertEquals(2, rejected[0]);
        maxSize[0] = 5;
        trigger.enqueue("test");
        assertEquals(3, rejected[0]);
    }
}
