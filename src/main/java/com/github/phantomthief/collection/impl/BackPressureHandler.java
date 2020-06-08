package com.github.phantomthief.collection.impl;

import java.util.concurrent.locks.Condition;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author w.vela
 * Created on 2019-07-30.
 */
class BackPressureHandler<T> implements RejectHandler<T> {

    private static final Logger logger = LoggerFactory.getLogger(BackPressureHandler.class);

    @Nullable
    private final BackPressureListener<T> listener;

    BackPressureHandler(BackPressureListener<T> listener) {
        this.listener = listener;
    }

    @Override
    public boolean onReject(T element, @Nullable Condition condition) {
        if (listener != null) {
            try {
                listener.onHandle(element);
            } catch (Throwable e) {
                logger.error("", e);
            }
        }
        assert condition != null;
        condition.awaitUninterruptibly();
        return true;
    }
}
