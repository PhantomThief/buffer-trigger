/**
 * 
 */
package com.github.phantomthief.collection;

/**
 * @author w.vela
 */
public interface BufferTrigger<E> {

    public void enqueue(E element);

    public void manuallyDoTrigger();
}
