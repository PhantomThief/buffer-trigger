/**
 * 
 */
package com.github.phantomthief.collection;

import java.util.function.Consumer;

import com.google.common.base.Throwables;

/**
 * @author w.vela
 */
public interface ThrowingConsumer<T> extends Consumer<T> {

    @Override
    public default void accept(final T t) {
        try {
            acceptThrows(t);
        } catch (Throwable e) {
            throw Throwables.propagate(e);
        }
    }

    public void acceptThrows(T t) throws Throwable;
}
