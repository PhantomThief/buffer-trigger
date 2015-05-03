/**
 * 
 */
package com.github.phantomthief.test;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.github.phantomthief.collection.impl.CollectionBufferTrigger;

/**
 * @author w.vela
 */
public class BufferTriggerTest {

    @Test
    public void test() throws InterruptedException {
        CollectionBufferTrigger<String, Collection<String>> buffer = CollectionBufferTrigger
                .<String, Collection<String>> newBuilder() //
                .on(5, TimeUnit.SECONDS, 10, i -> out("trig:1:" + i)) //
                .on(10, TimeUnit.SECONDS, 15, i -> out("trig:2:" + i)) //
                .fixedRate(6, TimeUnit.SECONDS, i -> out("trig:3:" + i)) //
                .build();
        Random rnd = new Random();
        for (int i = 0; i <= 100; i++) {
            String e = i + "";
            System.out.println("enqueue:" + i);
            buffer.enqueue(e);
            Thread.sleep(rnd.nextInt(1000));
        }
    }

    private final void out(Object obj) {
        System.out.println(
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis())
                        + "\t" + obj);
    }
}
