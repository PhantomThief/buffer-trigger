/**
 * 
 */
package com.github.phantomthief.test;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.collection.impl.SimpleBufferTrigger;

/**
 * @author w.vela
 */
public class MultiThreadBufferTriggerTest {

    private Set<String> dealed;

    @Test
    public void test() throws InterruptedException {
        BufferTrigger<String> buffer = SimpleBufferTrigger.<String, Set<String>> newBuilder() //
                .on(3, TimeUnit.SECONDS, 1) //
                .on(2, TimeUnit.SECONDS, 10) //
                .on(1, TimeUnit.SECONDS, 10000) //
                .consumer(this::out) //
                .setContainer(ConcurrentSkipListSet::new, Set::add) //
                .build();
        Set<String> allData = Collections.synchronizedSet(new HashSet<>());
        dealed = Collections.synchronizedSet(new HashSet<>());

        for (int i = 0; i < 10001; i++) {
            String e = "e:" + i;
            buffer.enqueue(e);
            allData.add(e);
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        for (int i = 0; i < 11; i++) {
            String e = "e:" + i;
            buffer.enqueue(e);
            allData.add(e);
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        for (int i = 0; i < 2; i++) {
            String e = "e:" + i;
            buffer.enqueue(e);
            allData.add(e);
        }

        buffer.manuallyDoTrigger();
        assert(dealed.equals(allData));
    }

    @Test
    public void test2() throws InterruptedException {
        BufferTrigger<String> buffer = SimpleBufferTrigger.<String, Set<String>> newBuilder() //
                .on(3, TimeUnit.SECONDS, 1) //
                .on(2, TimeUnit.SECONDS, 10) //
                .on(1, TimeUnit.SECONDS, 10000) //
                .setExceptionHandler((e, c) -> System.err.println(c)) //
                .consumer(this::exception) //
                .build();
        for (int i = 0; i < 1000; i++) {
            String e = "e:" + i;
            buffer.enqueue(e);
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(4));
        for (int i = 0; i < 2; i++) {
            String e = "e:" + i;
            buffer.enqueue(e);
        }
        buffer.manuallyDoTrigger();
    }

    @Test
    public void test3() throws InterruptedException {
        BufferTrigger<String> buffer = SimpleBufferTrigger.<String, Set<String>> newBuilder() //
                .on(1, TimeUnit.SECONDS, 1) //
                .on(2, TimeUnit.SECONDS, 2) //
                .consumer(this::delay) //
                .build();
        for (int i = 0; i < 10; i++) {
            String e = "e:" + i;
            buffer.enqueue(e);
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        for (int i = 0; i < 10; i++) {
            String e = "e:" + i;
            buffer.enqueue(e);
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        buffer.manuallyDoTrigger();
    }

    @Test
    public void test4() throws InterruptedException {
        BufferTrigger<String> buffer = SimpleBufferTrigger.<String, Set<String>> newBuilder() //
                .on(1, TimeUnit.SECONDS, 1) //
                .maxBufferCount(2) //
                .rejectHandler(e -> System.out.println("reject:" + e)) //
                .consumer(this::delay) //
                .build();
        for (int i = 0; i < 10; i++) {
            String e = "e:" + i;
            buffer.enqueue(e);
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        buffer.manuallyDoTrigger();
    }

    private final void exception(Set<String> obj) {
        throw new RuntimeException();
    }

    private final void delay(Collection<String> obj) {
        try {
            System.out.println("delayed:" + obj);
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        } catch (InterruptedException e) {
            // 
        }
    }

    private final void out(Set<String> obj) {
        System.out.println(
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis())
                        + "\t" + obj);
        dealed.addAll(obj);
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            //
        }
    }
}
