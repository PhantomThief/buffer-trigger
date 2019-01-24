package com.github.phantomthief.collection.impl;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.collection.BufferTrigger;

/**
 * @author w.vela
 * Created on 2017-11-13.
 */
class FixIntervalTest {

    private static final Logger logger = LoggerFactory.getLogger(FixIntervalTest.class);

    private static final ThreadLocal<Long> THREAD_LOCAL = new ThreadLocal<>();
    private static final int INTERVAL = 2;

    @Test
    void testDelayMode() {
        logger.info("start.");
        BufferTrigger<String> trigger = BufferTrigger.simpleTrigger() //
                .interval(INTERVAL, SECONDS) //
                .setContainer(HashSet::new, Set::add) //
                .consumer(this::consumerDelay) //
                .build();
        for (int i = 0; i < 25; i++) {
            trigger.enqueue(i + "");
            sleepUninterruptibly(1, SECONDS);
        }
    }

    private void consumerDelay(Set<String> strings) {
        logger.info("consumer delay.");
        long now = currentTimeMillis();
        Long lastTime = THREAD_LOCAL.get();
        THREAD_LOCAL.set(now);
        if (lastTime != null) {
            assertTrue(now - lastTime >= SECONDS.toMillis(INTERVAL));
        }
    }

    @Test
    void testFixedRateMode() {
        logger.info("start.");
        BufferTrigger<String> trigger = BufferTrigger.<String, Set<String>> simple() //
                .intervalAtFixedRate(INTERVAL, SECONDS) //
                .setContainer(HashSet::new, Set::add) //
                .consumer(this::consumerFixed) //
                .build();
        for (int i = 0; i < 20; i++) {
            trigger.enqueue(i + "");
            sleepUninterruptibly(1, SECONDS);
        }
    }

    private void consumerFixed(Set<String> strings) {
        logger.info("consumer fixed, check the log time manually.");
    }
}
