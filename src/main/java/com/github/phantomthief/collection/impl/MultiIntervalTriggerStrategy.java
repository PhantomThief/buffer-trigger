package com.github.phantomthief.collection.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

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

    private final SortedMap<Long, Long> triggerMap = new TreeMap<>();

    public MultiIntervalTriggerStrategy on(long interval, TimeUnit unit, long count) {
        triggerMap.put(unit.toMillis(interval), count);
        return this;
    }

    @Override
    public TriggerResult check(long lastConsumeTimestamp, long changedCount) {
        checkArgument(!triggerMap.isEmpty());

        boolean doTrig = false;
        long now = System.currentTimeMillis();
        long nextCheckTime = triggerMap.firstKey();

        for (Entry<Long, Long> entry : triggerMap.entrySet()) {
            if (now - lastConsumeTimestamp < entry.getKey()) {
                continue;
            }
            if (changedCount >= entry.getValue()) {
                doTrig = true;
                break;
            }
        }
        return TriggerResult.next(doTrig, nextCheckTime, MILLISECONDS);
    }
}
