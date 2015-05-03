/**
 * 
 */
package com.github.phantomthief.test;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.github.phantomthief.trigger.Trigger;
import com.github.phantomthief.trigger.impl.ScheduledTrigger;

/**
 * @author w.vela
 */
public class TriggerTest {

    @Test
    public void testTrigger2() throws InterruptedException {
        Trigger trigger = ScheduledTrigger.newBuilder()
                .on(5, TimeUnit.SECONDS, 10, () -> System.out.println("trig:1"))
                .on(1, TimeUnit.MINUTES, 100, () -> System.out.println("trig:2"))
                .fixedRate(3, TimeUnit.SECONDS, () -> System.out.println("trig:3")) //
                .build();
        for (int i = 0; i <= 5; i++) {
            System.out.println("trig.");
            trigger.markChange();
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(20));
        System.exit(0);
    }
}
