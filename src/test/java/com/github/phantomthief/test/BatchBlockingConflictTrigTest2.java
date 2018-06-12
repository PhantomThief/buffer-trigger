package com.github.phantomthief.test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.collection.BufferTrigger;

/**
 * @author w.vela
 * Created on 2018-06-12.
 */
class BatchBlockingConflictTrigTest2 {

    private static final Logger logger = LoggerFactory
            .getLogger(BatchBlockingConflictTrigTest2.class);
    private volatile boolean check = true;
    private volatile boolean failed;
    private final BufferTrigger<Integer> trigger = BufferTrigger.<Integer> batchBlocking() //
            .batchSize(100) //
            .bufferSize(1000) //
            .setConsumerEx(this::consumer) //
            .linger(3, SECONDS) //
            .build();

    @Disabled
    @Test
    void test() {
        check = false;
        for (int i = 0; i < 99; i++) {
            trigger.enqueue(i);
        }
        sleepUninterruptibly(3100, MILLISECONDS);
        check = true;
        for (int i = 0; i < 190; i++) {
            trigger.enqueue(i);
        }
        sleepUninterruptibly(3100, MILLISECONDS);
        check = false;
        for (int i = 0; i < 20; i++) {
            trigger.enqueue(i);
        }
        sleepUninterruptibly(5, SECONDS);
        assertFalse(failed);
    }

    private void consumer(List<Integer> strings) {
        if (check) {
            if (strings.size() < 100) {
                failed = true;
            }
        }
        logger.info("size:{}", strings.size());
        sleepUninterruptibly(1, SECONDS);
    }
}
