package com.github.phantomthief.collection.impl;

import static com.github.phantomthief.collection.impl.SimpleBufferTrigger.TriggerResult.trig;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.abs;
import static java.lang.Math.min;

import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import com.github.phantomthief.collection.impl.SimpleBufferTrigger.TriggerResult;
import com.github.phantomthief.collection.impl.SimpleBufferTrigger.TriggerStrategy;

/**
 * trigger like redis's rdb
 *
 * save 900 1
 * save 300 10
 * save 60 10000
 *
 * @author w.vela
 * Created on 15/07/2016.
 */
public class MultiIntervalTriggerStrategy implements TriggerStrategy {

    private long minTriggerPeriod = Long.MAX_VALUE;
    private final SortedMap<Long, Long> triggerMap = new TreeMap<>();

    public MultiIntervalTriggerStrategy on(long interval, TimeUnit unit, long count) {
        long intervalInMs = unit.toMillis(interval);
        triggerMap.put(intervalInMs, count);
        minTriggerPeriod = checkAndCalcMinPeriod();
        return this;
    }

    long minTriggerPeriod() { // for test case
        return minTriggerPeriod;
    }

    private long checkAndCalcMinPeriod() {
        long minPeriod = Long.MAX_VALUE;
        Long maxTrigChangeCount = null;
        long lastPeriod = 0;

        for (Entry<Long, Long> entry : triggerMap.entrySet()) {
            long period = entry.getKey();
            minPeriod = min(minPeriod, period);
            if (lastPeriod > 0) {
                minPeriod = min(minPeriod, abs(lastPeriod - period));
            }
            long trigChangedCount = entry.getValue();
            if (maxTrigChangeCount == null) {
                maxTrigChangeCount = trigChangedCount;
            } else {
                if (maxTrigChangeCount <= trigChangedCount) {
                    throw new IllegalArgumentException(
                            "found invalid trigger setting:" + triggerMap);
                }
            }
            lastPeriod = period;
        }
        return minPeriod;
    }

    @Override
    public TriggerResult canTrigger(long lastConsumeTimestamp, long changedCount) {
        checkArgument(!triggerMap.isEmpty());

        boolean doConsumer = false;

        long now = System.currentTimeMillis();

        for (Entry<Long, Long> entry : triggerMap.entrySet()) {
            if (now - lastConsumeTimestamp < entry.getKey()) {
                continue;
            }
            if (changedCount >= entry.getValue()) {
                doConsumer = true;
                break;
            }
        }
        return trig(doConsumer, minTriggerPeriod);
    }
}
