package com.github.phantomthief.test;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.collection.BufferTrigger;

public class TestBufferTriggerAutoClosed {

    @Test
    void test() {
        Duration[] linger = {ofSeconds(2)};
        int[] consumed = {0};
        try (BufferTrigger<String> buffer = BufferTrigger.<String> batchBlocking()
                .batchSize(100)
                .linger(() -> linger[0])
                .setConsumerEx(it -> consumed[0]++)
                .build()) {
            buffer.enqueue("Hi, I am ok.");
        } catch (Exception e) {

        }
        assertEquals(consumed[0], 1);
    }
}
