package com.pdomingo.pipeline.transform;

/**
 * Interfaz de adaptacion básica
 * @param <T> tipo del que convertir
 * @param <U> tipo al que convertir
 */
public interface Transformer<T, U> {

    /**
     * Transforma un objeto {@param t} de entrada de {@link T}
     * a un objeto de tipo {@link U}
     * @param t objeto de entrada de tipo T
     * @return objeto de salida de tipo U
     */
    U transform(T t);
}