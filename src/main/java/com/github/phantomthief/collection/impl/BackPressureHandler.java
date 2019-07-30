package com.github.phantomthief.collection.impl;

import java.util.concurrent.locks.Condition;

import javax.annotation.Nullable;

import com.github.phantomthief.collection.RejectHandler;

/**
 * @author w.vela
 * Created on 2019-07-30.
 */
class BackPressureHandler<T> implements RejectHandler<T> {

    @Override
    public boolean onReject(T element, @Nullable Condition condition) throws Throwable {
        assert condition != null;
        condition.awaitUninterruptibly();
        return true;
    }
}
