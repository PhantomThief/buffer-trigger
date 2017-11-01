package com.github.phantomthief.test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.util.TickerBatchInvoker;

/**
 * @author w.vela
 * Created on 16/5/21.
 */
class TickerBatchInvokerTest {

    private Map<Integer, String> load(Collection<Integer> keys) {
        System.out.println("invoke:" + keys);
        sleepUninterruptibly(2, SECONDS);
        return keys.stream().collect(toMap(identity(), Object::toString));
    }

    private Map<Integer, String> loadEmpty(Collection<Integer> keys) {
        System.out.println("invoke empty:" + keys);
        return emptyMap();
    }

    private Map<Integer, String> loadException(Collection<Integer> keys)
            throws IllegalStateException {
        System.out.println("invoke empty:" + keys);
        throw new IllegalStateException("test");
    }

    @Test
    void test() {
        TickerBatchInvoker<Integer, String> batchInvoker = TickerBatchInvoker.newBuilder()
                .build(this::load);
        Map<Integer, CompletableFuture<String>> futures = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            CompletableFuture<String> apply = batchInvoker.apply(i);
            futures.put(i, apply);
            sleepUninterruptibly(100, MILLISECONDS);
        }
        futures.forEach((i, future) -> {
            try {
                String s = future.get();
                assertEquals(s, String.valueOf(i));
            } catch (Throwable e) {
                fail(e);
            }
        });
    }

    @Test
    void testTimeout() {
        TickerBatchInvoker<Integer, String> batchInvoker = TickerBatchInvoker.newBuilder()
                .build(this::load);
        Map<Integer, CompletableFuture<String>> futures = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            CompletableFuture<String> apply = batchInvoker.apply(i);
            futures.put(i, apply);
            sleepUninterruptibly(100, MILLISECONDS);
        }
        futures.forEach((i, future) -> assertThrows(TimeoutException.class,
                () -> future.get(100, MILLISECONDS)));
    }

    @Test
    void testEmptyData() {
        TickerBatchInvoker<Integer, String> batchInvoker = TickerBatchInvoker.newBuilder()
                .build(this::loadEmpty);
        Map<Integer, CompletableFuture<String>> futures = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            CompletableFuture<String> apply = batchInvoker.apply(i);
            futures.put(i, apply);
            sleepUninterruptibly(100, MILLISECONDS);
        }
        futures.forEach((i, future) -> {
            try {
                assertNull(future.get());
            } catch (Throwable e) {
                fail(e);
            }
        });
    }

    @Test
    void testException() {
        TickerBatchInvoker<Integer, String> batchInvoker = TickerBatchInvoker.newBuilder()
                .build(this::loadException);
        Map<Integer, CompletableFuture<String>> futures = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            CompletableFuture<String> apply = batchInvoker.apply(i);
            futures.put(i, apply);
            sleepUninterruptibly(100, MILLISECONDS);
        }
        futures.forEach((i, future) -> {
            Throwable e = assertThrows(Throwable.class, future::get);
            assertEquals("test", e.getCause().getMessage());
        });
    }
}
