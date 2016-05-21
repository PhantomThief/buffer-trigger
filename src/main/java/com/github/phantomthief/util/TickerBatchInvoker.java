package com.github.phantomthief.util;

import static com.github.phantomthief.tuple.Tuple.tuple;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.github.phantomthief.collection.BufferTrigger;
import com.github.phantomthief.collection.impl.SimpleBufferTrigger;
import com.github.phantomthief.tuple.TwoTuple;

/**
 * @author w.vela
 * Created on 16/5/21.
 */
public class TickerBatchInvoker<K, V> implements Function<K, CompletableFuture<V>> {

    private final ThrowableFunction<Collection<K>, Map<K, V>, ? extends Throwable> batchInvoker;
    private final ExecutorService executor;
    private final BufferTrigger<TwoTuple<K, CompletableFuture<V>>> bufferTrigger;

    private TickerBatchInvoker(long ticker,
            ThrowableFunction<Collection<K>, Map<K, V>, ? extends Throwable> batchInvoker,
            ExecutorService executor) {
        this.batchInvoker = batchInvoker;
        this.executor = executor;
        this.bufferTrigger = SimpleBufferTrigger.newBuilder() //
                .setContainer(ConcurrentHashMap::new, this::enqueue) //
                .on(ticker, MILLISECONDS, 1) //
                .consumer(this::batchInvoke) //
                .build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    private boolean enqueue(Map<K, List<CompletableFuture<V>>> map,
            TwoTuple<K, CompletableFuture<V>> e) {
        map.compute(e.getFirst(), (k, list) -> {
            if (list == null) {
                list = new ArrayList<>();
            }
            synchronized (list) {
                list.add(e.getSecond());
            }
            return list;
        });
        return true;
    }

    private void batchInvoke(Map<K, List<CompletableFuture<V>>> map) {
        executor.execute(() -> {
            try {
                Map<K, V> result = batchInvoker.apply(map.keySet());
                map.forEach((key, futures) -> {
                    V v = result.get(key);
                    // 虽然框架会尽力保证enqueue/consume是互斥的,但是这里还是重复保证下
                    synchronized (futures) {
                        for (CompletableFuture<V> future : futures) {
                            future.complete(v);
                        }
                    }
                });
            } catch (Throwable e) {
                for (List<CompletableFuture<V>> futures : map.values()) {
                    synchronized (futures) {
                        futures.stream() //
                                .filter(future -> !future.isDone()) //
                                .forEach(future -> future.completeExceptionally(e));
                    }
                }
            }
        });
    }

    @Override
    public CompletableFuture<V> apply(K key) {
        CompletableFuture<V> future = new CompletableFuture<>();
        bufferTrigger.enqueue(tuple(key, future));
        return future;
    }

    public static class Builder {

        private long ticker;
        private ExecutorService executorService;

        private Builder() {
        }

        public Builder ticker(long time, TimeUnit unit) {
            this.ticker = unit.toMillis(time);
            return this;
        }

        public Builder executor(ExecutorService executor) {
            this.executorService = executor;
            return this;
        }

        public Builder threads(int nThreads) {
            this.executorService = Executors.newFixedThreadPool(nThreads);
            return this;
        }

        public <K, V> TickerBatchInvoker<K, V> build(
                ThrowableFunction<Collection<K>, Map<K, V>, ? extends Throwable> batchInvoker) {
            checkNotNull(batchInvoker);
            ensure();
            return new TickerBatchInvoker<>(ticker, batchInvoker, executorService);
        }

        private void ensure() {
            if (ticker <= 0) {
                ticker = SECONDS.toMillis(1);
            }
            if (executorService == null) {
                executorService = Executors.newCachedThreadPool();
            }
        }
    }
}
