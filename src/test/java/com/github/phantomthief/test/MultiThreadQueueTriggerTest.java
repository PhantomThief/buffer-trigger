package com.github.phantomthief.test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.Collections.synchronizedSet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.collection.BufferTrigger;

/**
 * @author w.vela
 */
class MultiThreadQueueTriggerTest {

    private static final Logger logger = LoggerFactory.getLogger(MultiThreadQueueTriggerTest.class);
    private Set<String> deal;

    @Test
    void test() {
        BufferTrigger<String> buffer = BufferTrigger.<String> batchBlocking() //
                .batchSize(3) //
                .setConsumerEx(this::delay) //
                .build();
        Set<String> allData = synchronizedSet(new HashSet<>());
        deal = synchronizedSet(new HashSet<>());

        for (int i = 0; i < 20; i++) {
            String e = "e:" + i;
            logger.info("enqueue:{}", e);
            buffer.enqueue(e);
            allData.add(e);
        }
        logger.info("after enqueue.");
        sleepUninterruptibly(1, SECONDS);

        logger.info("do manually");
        buffer.manuallyDoTrigger();
        logger.info("after do manually");
        assertEquals(deal, allData);
    }

    @Test
    void test2() {
        BufferTrigger<String> buffer = BufferTrigger.<String> batchBlocking() //
                .batchSize(3) //
                .setConsumerEx(this::delay) //
                .linger(10, MILLISECONDS) //
                .build();
        Set<String> allData = synchronizedSet(new HashSet<>());
        deal = synchronizedSet(new HashSet<>());

        for (int i = 0; i < 30; i++) {
            String e = "e:" + i;
            logger.info("enqueue:{}", e);
            buffer.enqueue(e);
            allData.add(e);
            sleepUninterruptibly(10, MILLISECONDS);
        }
        logger.info("after enqueue.");
        sleepUninterruptibly(1, SECONDS);

        logger.info("do manually");
        buffer.manuallyDoTrigger();
        logger.info("after do manually");
        assertEquals(deal, allData);
    }

    @Test
    void test3() {
        BufferTrigger<String> buffer = BufferTrigger.<String> batchBlocking() //
                .batchSize(3) //
                .setConsumerEx(this::delay) //
                .bufferSize(10) //
                .linger(1, SECONDS) //
                .build();
        Set<String> allData = synchronizedSet(new HashSet<>());
        deal = synchronizedSet(new HashSet<>());

        for (int i = 0; i < 30; i++) {
            String e = "e:" + i;
            logger.info("enqueue:{}", e);
            buffer.enqueue(e);
            allData.add(e);
            sleepUninterruptibly(10, MILLISECONDS);
        }
        logger.info("after enqueue.");
        sleepUninterruptibly(1, SECONDS);

        logger.info("do manually");
        buffer.manuallyDoTrigger();
        logger.info("after do manually");
        assertEquals(deal, allData);
    }

    private void delay(Collection<String> obj) {
        logger.info("delayed:{}", obj);
        sleepUninterruptibly(2, SECONDS);
        deal.addAll(obj);
        logger.info("after:{}", obj);
    }
}
