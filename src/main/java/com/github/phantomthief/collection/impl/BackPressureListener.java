package com.github.phantomthief.collection.impl;

/**
 * @author w.vela
 * Created on 2020-06-08.
 */
public interface BackPressureListener<T> {

    void onHandle(T element);
}
