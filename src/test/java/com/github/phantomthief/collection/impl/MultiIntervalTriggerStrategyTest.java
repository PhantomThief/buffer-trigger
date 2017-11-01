package com.github.phantomthief.collection.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * @author w.vela
 * Created on 05/08/2016.
 */
class MultiIntervalTriggerStrategyTest {

    @Test
    void test() {
        MultiIntervalTriggerStrategy multiIntervalTriggerStrategy = new MultiIntervalTriggerStrategy();
        multiIntervalTriggerStrategy.on(5, SECONDS, 10);
        multiIntervalTriggerStrategy.on(6, SECONDS, 8);
        multiIntervalTriggerStrategy.on(10, SECONDS, 3);
        assertEquals(multiIntervalTriggerStrategy.minTriggerPeriod(), SECONDS.toMillis(1));
        multiIntervalTriggerStrategy.on(500, MILLISECONDS, 999);
        assertEquals(multiIntervalTriggerStrategy.minTriggerPeriod(), 500);
    }
}
