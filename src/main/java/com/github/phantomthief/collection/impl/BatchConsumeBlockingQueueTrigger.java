package com.github.phantomthief.collection.impl;

import static com.github.phantomthief.concurrent.MoreFutures.scheduleWithDynamicDelay;
import static com.github.phantomthief.util.MoreLocks.runWithLock;
import static com.github.phantomthief.util.MoreLocks.runWithTryLock;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static com.google.common.util.concurrent.Uninterruptibles.putUninterruptibly;
import static java.lang.Integer.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.slf4j.Logger;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.util.ThrowableConsumer;

/**
 * {@link BufferTrigger}基于阻塞队列的批量消费触发器实现.
 * <p>
 * 该触发器适合生产者-消费者场景，缓存容器基于{@link LinkedBlockingQueue}队列实现.
 * <p>
 * 触发策略类似Kafka linger，批处理阈值与延迟等待时间满足其一即触发消费回调.
 * @author w.vela
 */
public class BatchConsumeBlockingQueueTrigger<E> implements BufferTrigger<E> {

    private static final Logger logger = getLogger(BatchConsumeBlockingQueueTrigger.class);

    private final BlockingQueue<E> queue;
    private final int batchSize;
    private final ThrowableConsumer<List<E>, Exception> consumer;
    private final BiConsumer<Throwable, List<E>> exceptionHandler;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicBoolean running = new AtomicBoolean();
    private final Runnable shutdownExecutor;

    private volatile boolean shutdown;

    BatchConsumeBlockingQueueTrigger(BatchConsumerTriggerBuilder<E> builder) {
        Supplier<Duration> linger = builder.linger;
        this.batchSize = builder.batchSize;
        this.queue = new LinkedBlockingQueue<>(max(builder.bufferSize, batchSize));
        this.consumer = builder.consumer;
        this.exceptionHandler = builder.exceptionHandler;
        this.scheduledExecutorService = builder.scheduledExecutorService;
        Future<?> future = scheduleWithDynamicDelay(scheduledExecutorService, linger, () -> doBatchConsumer(TriggerType.LINGER));
        this.shutdownExecutor = () -> {
            future.cancel(false);
            if (builder.usingInnerExecutor) {
                shutdownAndAwaitTermination(builder.scheduledExecutorService, 1, DAYS);
            }
        };
    }

    /**
     * 该方法即将废弃，可更换为{@link com.github.phantomthief.collection.BufferTrigger#batchBlocking}.
     */
    @Deprecated
    public static BatchConsumerTriggerBuilder<Object> newBuilder() {
        return new BatchConsumerTriggerBuilder<>();
    }

    /**
     * 将需要定时处理的元素推入队列.
     * <p>
     * 新元素入队列后，会检测一次当前队列元素数量，如满足批处理阈值，会触发一次消费回调.
     */
    @Override
    public void enqueue(E element) {
        checkState(!shutdown, "buffer trigger was shutdown.");

        putUninterruptibly(queue, element);
        tryTrigBatchConsume();
    }

    private void tryTrigBatchConsume() {
        if (queue.size() >= batchSize) {
            runWithTryLock(lock, () -> {
                if (queue.size() >= batchSize) {
                    if (!running.get()) { // prevent repeat enqueue
                        this.scheduledExecutorService.execute(() -> doBatchConsumer(TriggerType.ENQUEUE));
                        running.set(true);
                    }
                }
            });
        }
    }

    @Override
    public void manuallyDoTrigger() {
        doBatchConsumer(TriggerType.MANUALLY);
    }

    private void doBatchConsumer(TriggerType type) {
        runWithLock(lock, () -> {
            try {
                running.set(true);
                int queueSizeBeforeConsumer = queue.size();
                int consumedSize = 0;
                while (!queue.isEmpty()) {
                    if (queue.size() < batchSize) {
                        if (type == TriggerType.ENQUEUE) {
                            return;
                        } else if (type == TriggerType.LINGER
                                && consumedSize >= queueSizeBeforeConsumer) {
                            return;
                        }
                    }
                    List<E> toConsumeData = new ArrayList<>(min(batchSize, queue.size()));
                    queue.drainTo(toConsumeData, batchSize);
                    if (!toConsumeData.isEmpty()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("do batch consumer:{}, size:{}", type,
                                    toConsumeData.size());
                        }
                        consumedSize += toConsumeData.size();
                        doConsume(toConsumeData);
                    }
                }
            } finally {
                running.set(false);
            }
        });
    }

    private void doConsume(List<E> toConsumeData) {
        try {
            consumer.accept(toConsumeData);
        } catch (Throwable e) {
            if (exceptionHandler != null) {
                try {
                    exceptionHandler.accept(e, toConsumeData);
                } catch (Throwable ex) {
                    logger.error("", ex);
                }
            } else {
                logger.error("Ops.", e);
            }
        }
    }

    @Override
    public long getPendingChanges() {
        return queue.size();
    }

    @Override
    public void close() {
        shutdown = true;
        try {
            manuallyDoTrigger();
        } finally {
            shutdownExecutor.run();
        }
    }

    /**
     * just for log and debug
     */
    private enum TriggerType {
        LINGER, ENQUEUE, MANUALLY
    }
}
