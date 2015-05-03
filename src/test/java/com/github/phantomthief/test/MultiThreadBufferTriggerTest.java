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

import com.github.phantomthief.collection.impl.BaseBufferTrigger;

/**
 * @author w.vela
 */
public class MultiThreadBufferTriggerTest {

    private Set<String> dealed;

    @Test
    public void test() throws InterruptedException {
        BaseBufferTrigger<String> buffer = BaseBufferTrigger.<String, List<String>> newBuilder() //
                .on(1, TimeUnit.SECONDS, 10, this::out) //
                //                .on(10, TimeUnit.SECONDS, 15, this::out) //
                //                .fixedRate(6, TimeUnit.SECONDS, this::out) //
                .setContainer(() -> Collections.synchronizedList(new ArrayList<String>()),
                        List::add) //
                .build();
        Set<String> allData = Collections.synchronizedSet(new HashSet<>());
        dealed = Collections.synchronizedSet(new HashSet<>());
        List<Thread> threads = new ArrayList<>();
        for (int j = 0; j <= 10; j++) {
            int base = j;
            Thread t = new Thread() {

                @Override
                public void run() {
                    for (int i = 0; i <= 10; i++) {
                        String e = (base * 10000 + i) + "";
                        allData.add(e);
                        System.out.println("enqueue:" + e);
                        buffer.enqueue(e);
                    }
                }

            };
            threads.add(t);
            t.start();
        }
        threads.forEach(t -> {
            try {
                t.join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } );
        System.out.println(dealed);
        System.out.println(allData);
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
