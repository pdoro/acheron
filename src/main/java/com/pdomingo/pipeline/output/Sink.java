package com.pdomingo.pipeline.output;

/**
 * Representa un sumidero, una interfaz que permite
 * escribir objetos que tendran un destino final (
 * un fichero, la red, salida estandar, etc)
 * @param <T> tipo
 */
public interface Sink<T> {

    /**
     * Manda o escribe el {@code item} a traves del sumidero
     * @param item a ser escrito
     */
    void write(T item);
}