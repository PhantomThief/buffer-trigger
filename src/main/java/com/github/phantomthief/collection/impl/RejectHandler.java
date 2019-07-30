package com.github.phantomthief.collection.impl;

import java.util.concurrent.locks.Condition;

import javax.annotation.Nullable;

/**
 * @author w.vela
 * Created on 2019-07-30.
 */
interface RejectHandler<T> {

    /**
     * execute on caller thread
     * @return {@code true} 继续执行, {@code false} 阻止执行
     */
    boolean onReject(T element, @Nullable Condition condition) throws Throwable;
}
