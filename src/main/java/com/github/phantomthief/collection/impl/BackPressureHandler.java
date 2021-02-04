package com.github.phantomthief.collection.impl;

import static java.lang.System.nanoTime;

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

    private static GlobalBackPressureListener globalBackPressureListener = null;

    @Nullable
    private final BackPressureListener<T> listener;
    private String name;

    BackPressureHandler(BackPressureListener<T> listener) {
        this.listener = listener;
    }

    void setName(String name) {
        this.name = name;
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
        if (globalBackPressureListener != null) {
            try {
                globalBackPressureListener.onHandle(name, element);
            } catch (Throwable e) {
                logger.error("", e);
            }
        }
        assert condition != null;
        long startNano = nanoTime();
        condition.awaitUninterruptibly();
        long blockInNano = nanoTime() - startNano;
        if (globalBackPressureListener != null) {
            try {
                globalBackPressureListener.postHandle(name, element, blockInNano);
            } catch (Throwable e) {
                logger.error("", e);
            }
        }
        return true;
    }

    static void setupGlobalBackPressureListener(GlobalBackPressureListener listener) {
        globalBackPressureListener = listener;
    }
}
