/**
 * 
 */
package com.github.phantomthief.collection;

/**
 * @author w.vela
 */
public interface BufferTrigger<E> {

    void enqueue(E element);

    void manuallyDoTrigger();

    long getPendingChanges();
}
