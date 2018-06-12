package com.github.phantomthief.test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.github.phantomthief.collection.BufferTrigger;

/**
 * @author w.vela
 * Created on 2018-06-12.
 */
class BatchBlockingConflictTrigTest {

    private ExecutorService executor = new ThreadPoolExecutor(2, 2, 0L, MILLISECONDS,
            new LinkedBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
    private volatile boolean check = true;
    private volatile boolean failed;
    private final BufferTrigger<Integer> trigger = BufferTrigger.<Integer> batchBlocking() //
            .batchSize(100) //
            .bufferSize(1000) //
            .setConsumerEx(this::consumer) //
            .linger(1, SECONDS) //
            .build();

    @Disabled
    @Test
    void test() throws InterruptedException {
        Thread thread = new Thread(() -> {
            while (true) {
                trigger.enqueue(ThreadLocalRandom.current().nextInt());
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        thread.start();
        thread.join(SECONDS.toMillis(5), 0);
        thread.interrupt();
        check = false;
        sleepUninterruptibly(3, SECONDS);
        assertFalse(failed);
    }

    private void consumer(List<Integer> strings) {
        executor.execute(() -> {
            if (check) {
                if (strings.size() != 100) {
                    fail("size:" + strings.size());
                    failed = true;
                }
            }
            trigger.enqueue(0);
        });
    }
}
