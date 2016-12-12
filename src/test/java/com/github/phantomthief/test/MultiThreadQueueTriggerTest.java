/**
 * 
 */
package com.github.phantomthief.test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.Collections.synchronizedSet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.github.phantomthief.collection.BufferTrigger;

/**
 * @author w.vela
 */
public class MultiThreadQueueTriggerTest {

    private Set<String> deal;

    @Test
    public void test() throws InterruptedException {
        BufferTrigger<String> buffer = BufferTrigger.<String> batchBlocking() //
                .batchConsumerSize(3) //
                .setConsumerEx(this::delay) //
                .build();
        Set<String> allData = synchronizedSet(new HashSet<>());
        deal = synchronizedSet(new HashSet<>());

        for (int i = 0; i < 20; i++) {
            String e = "e:" + i;
            System.out.println("enqueue:" + e);
            buffer.enqueue(e);
            allData.add(e);
        }
        System.out.println("after enqueue.");
        sleepUninterruptibly(1, SECONDS);

        System.out.println("do manually");
        buffer.manuallyDoTrigger();
        System.out.println("after do manually");
        assertTrue(deal.equals(allData));
    }

    @Test
    public void test2() throws InterruptedException {
        BufferTrigger<String> buffer = BufferTrigger.<String> batchBlocking() //
                .batchConsumerSize(3) //
                .setConsumerEx(this::delay) //
                .consumePeriod(10, MILLISECONDS) //
                .build();
        Set<String> allData = synchronizedSet(new HashSet<>());
        deal = synchronizedSet(new HashSet<>());

        for (int i = 0; i < 30; i++) {
            String e = "e:" + i;
            System.out.println("enqueue:" + e);
            buffer.enqueue(e);
            allData.add(e);
            sleepUninterruptibly(10, MILLISECONDS);
        }
        System.out.println("after enqueue.");
        sleepUninterruptibly(1, SECONDS);

        System.out.println("do manually");
        buffer.manuallyDoTrigger();
        System.out.println("after do manually");
        assertTrue(deal.equals(allData));
    }

    private void delay(Collection<String> obj) {
        System.out.println("delayed:" + obj);
        sleepUninterruptibly(2, SECONDS);
        deal.addAll(obj);
        System.out.println("after:" + obj);
    }
}
