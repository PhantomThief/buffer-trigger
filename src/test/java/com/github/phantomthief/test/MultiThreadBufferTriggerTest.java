/**
 * 
 */
package com.github.phantomthief.test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
        BufferTrigger<String> buffer = SimpleBufferTrigger.<String, List<String>> newBuilder() //
                .on(3, TimeUnit.SECONDS, 1) //
                .on(2, TimeUnit.SECONDS, 10) //
                .on(1, TimeUnit.SECONDS, 10000) //
                .consumer(this::out) //
                .setContainer(() -> Collections.synchronizedList(new ArrayList<String>()),
                        List::add) //
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

    private final void out(List<String> obj) {
        System.out.println(
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis())
                        + "\t" + obj);
        dealed.addAll(obj);
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
