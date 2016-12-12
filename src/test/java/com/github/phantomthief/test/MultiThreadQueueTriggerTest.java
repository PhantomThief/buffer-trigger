/**
 * 
 */
package com.github.phantomthief.test;

import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.collection.impl.BatchConsumeBlockingQueueTrigger;

/**
 * @author w.vela
 */
public class MultiThreadQueueTriggerTest {

    private Set<String> dealed;

    @Test
    public void test() throws InterruptedException {
        BufferTrigger<String> buffer = BatchConsumeBlockingQueueTrigger.newBuilder() //
                .batchConsumerSize(3) //
                .setConsumer(this::delay) //
                .queueCapacity(5) //
                .build();
        Set<String> allData = Collections.synchronizedSet(new HashSet<>());
        dealed = Collections.synchronizedSet(new HashSet<>());

        for (int i = 0; i < 20; i++) {
            String e = "e:" + i;
            System.out.println("enqueue:" + e);
            buffer.enqueue(e);
            allData.add(e);
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        buffer.manuallyDoTrigger();
        assertTrue(dealed.equals(allData));
    }

    @Test
    public void test2() throws InterruptedException {
        BufferTrigger<String> buffer = BufferTrigger.<String> batchBlocking() //
                .batchConsumerSize(3) //
                .setConsumer(this::delay) //
                .build();
        Set<String> allData = Collections.synchronizedSet(new HashSet<>());
        dealed = Collections.synchronizedSet(new HashSet<>());

        for (int i = 0; i < 21; i++) {
            String e = "e:" + i;
            System.out.println("enqueue:" + e);
            buffer.enqueue(e);
            allData.add(e);
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        buffer.manuallyDoTrigger();
        assertTrue(dealed.equals(allData));
    }

    private void delay(Collection<String> obj) {
        try {
            System.out.println("delayed:" + obj);
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
            dealed.addAll(obj);
        } catch (InterruptedException e) {
            // 
        }
    }
}
