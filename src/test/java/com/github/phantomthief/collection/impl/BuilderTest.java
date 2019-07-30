package com.github.phantomthief.collection.impl;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.collection.BufferTrigger;

/**
 * @author w.vela
 * Created on 2019-07-30.
 */
class BuilderTest {

    @Test
    void testBuilder() {
        assertThrows(NullPointerException.class, () ->
                BufferTrigger.simple()
                        .build());
        assertThrows(IllegalStateException.class, () ->
                BufferTrigger.simple()
                        .consumer(it -> {})
                        .enableBackPressure()
                        .disableSwitchLock()
                        .build());
        assertThrows(IllegalStateException.class, () ->
                BufferTrigger.simple()
                        .consumer(it -> {})
                        .enableBackPressure()
                        .rejectHandlerEx((it, c) -> true)
                        .build());
        assertThrows(IllegalStateException.class, () ->
                BufferTrigger.simple()
                        .consumer(it -> {})
                        .rejectHandlerEx((it, c) -> true)
                        .enableBackPressure()
                        .build());
    }
}
