/**
 * 
 */
package com.github.phantomthief.collection;

/**
 * @author w.vela
 */
public interface BufferTrigger<E> {

    public void enqueue(E element, long weight);

    public default void enqueue(E element) {
        enqueue(element, 1);
    }

    public void manuallyDoTrigger();
}
