package com.github.phantomthief.test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.collection.impl.SimpleBufferTrigger;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author w.vela
 * Created on 16/5/7.
 */
public class ConflictReadWriteTest {

    private final WeakHashMap<List<Integer>, AtomicInteger> counter = new WeakHashMap<>();

    @Test
    public void test() {
        Random random = new Random();
        BufferTrigger<Integer> bufferTrigger = SimpleBufferTrigger
                .<Integer, List<Integer>> newGenericBuilder().on(5, SECONDS, 1) //
                .setContainerEx(ArrayList::new, (e, c) -> {
                    AtomicInteger atomicInteger;
                    synchronized (counter) {
                        atomicInteger = counter.computeIfAbsent(e, i -> new AtomicInteger());
                    }
                    atomicInteger.incrementAndGet();
                    try {
                        sleepUninterruptibly(random.nextInt(1000), MILLISECONDS);
                        synchronized (e) {
                            e.add(c);
                        }
                    } finally {
                        atomicInteger.decrementAndGet();
                    }
                    return 1;
                }) //
                .consumer(list -> {
                    System.out.println("start consume:" + list.hashCode() + ", size:" + list);
                    AtomicInteger atomicInteger;
                    synchronized (counter) {
                        atomicInteger = counter.get(list);
                    }
                    Assert.assertEquals(0, atomicInteger.get());
                    sleepUninterruptibly(2, SECONDS);
                    synchronized (counter) {
                        atomicInteger = counter.get(list);
                    }
                    Assert.assertEquals(0, atomicInteger.get());
                    System.out.println("end consume:" + list.hashCode() + ", size:" + list);
                }) //
                .build();
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        for (int i = 0; i < 100000; i++) {
            executorService.execute(() -> bufferTrigger.enqueue(random.nextInt(10000)));
        }
        MoreExecutors.shutdownAndAwaitTermination(executorService, 1, DAYS);
    }
}
