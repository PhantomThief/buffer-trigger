package com.github.phantomthief.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.collection.BufferTrigger;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * @author xuechuandong
 * Created on 2022-07-29
 */
public class SimpleBufferTriggerRejectHandlerTest {
    private AtomicLong enqueueCount = new AtomicLong();
    private AtomicLong consumeCount = new AtomicLong();
    private AtomicLong rejectCount = new AtomicLong();

    private BufferTrigger<String> buffer = BufferTrigger.<String, Queue<String>>simple()
            .name("test-trigger")
            .setContainer(ConcurrentLinkedQueue::new, Queue::add)
            .maxBufferCount(1000)
            .interval(1, TimeUnit.SECONDS)
            .consumer(this::doBatchReload)
            .rejectHandler(this::onTaskRejected)
            .build();


    private void doBatchReload(Iterable<String> values) {
        consumeCount.addAndGet(Iterables.size(values));
        Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(1000));
    }

    private void onTaskRejected(String value) {
        rejectCount.addAndGet(1);
    }

    @Test
    void test() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(100);
        int count = 1000000;
        for (int i = 0; i < count; i++) {
            executor.submit(() -> {
                enqueueCount.getAndAdd(1);
                buffer.enqueue("test");
            });
            if (i % 353 == 0) {
                Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(50));
            }
        }
        executor.shutdown();
        boolean finished = executor.awaitTermination(30, TimeUnit.SECONDS);
        assertTrue(finished);
        buffer.manuallyDoTrigger();
        assertEquals(count, enqueueCount.get());
        assertEquals(count, consumeCount.get() + rejectCount.get());
    }
}
