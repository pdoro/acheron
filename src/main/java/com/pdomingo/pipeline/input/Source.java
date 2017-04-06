package com.pdomingo.pipeline.input;

/**
 * Representa una fuente de datos, una interfaz que permite
 * leer objetos desde un punto de entrada indeterminado (
 * un fichero, la red, salida estandar, etc)
 * @param <T> tipo
 */
public interface Source<T> {

    /**
     * Lee un objeto de la fuente
     * @return objeto leido desde la fuente
     */
    T read();
}
