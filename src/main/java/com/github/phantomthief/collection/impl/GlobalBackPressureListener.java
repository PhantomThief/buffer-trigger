package com.github.phantomthief.collection.impl;

import javax.annotation.Nullable;

/**
 * @author w.vela
 * Created on 2021-02-04.
 */
public interface GlobalBackPressureListener {

    void onHandle(@Nullable String name, Object element);

    void postHandle(@Nullable String name, Object element, long blockInNano);
}
