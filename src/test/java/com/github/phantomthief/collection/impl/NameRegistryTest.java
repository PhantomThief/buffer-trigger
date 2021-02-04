package com.github.phantomthief.collection.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.collection.BufferTrigger;

/**
 * @author w.vela
 * Created on 2021-02-04.
 */
class NameRegistryTest {

    private static final String[] NAME = {null};

    static {
        SimpleBufferTrigger.setupGlobalNameRegistry(() -> {
            String name1 = NameRegistry.autoRegistry().name();
            NAME[0] = name1;
            return name1;
        });
    }

    private BufferTrigger<String> buffer1 = BufferTrigger.<String, AtomicLong> simple()
            .setContainer(AtomicLong::new, (counter, _2) -> {
                counter.incrementAndGet();
                return true;
            })
            .consumer(it -> {})
            .build();

    @Test
    void test() {
        assertEquals("NameRegistryTest.java:34", NAME[0]); // 是 buffer1 声明的行数

        BufferTrigger<String> buffer2 = BufferTrigger.simpleTrigger()
                .setContainer(AtomicLong::new, (counter, _2) -> {
                    counter.incrementAndGet();
                    return true;
                })
                .consumer(it -> {})
                .build();
        assertNull(NAME[0]);
    }
}
