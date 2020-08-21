package com.github.phantomthief.collection.impl;

import static com.github.phantomthief.collection.impl.SimpleBufferTrigger.TriggerResult.trig;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.abs;
import static java.lang.Math.min;
import static java.lang.System.currentTimeMillis;

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
 * 注意，这里的触发条件和 Kafka Producer 的 linger 不一样，并不是 时间和次数达到一个就触发一次
 * 而是指定时间内累计到特定次数才触发，所以一般会有一个兜底的 1 次来做时间触发
 *
 * 更进一步，我们的场景大多数时候一般使用 {@link SimpleBufferTriggerBuilder#interval} 触发就能满足业务的基本需求
 * 所以目前 {@link SimpleBufferTrigger} 没有提供类 Kafka Producer 的 linger 触发模式；
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

        long now = currentTimeMillis();

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
