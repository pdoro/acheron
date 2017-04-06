package com.pdomingo.pipeline.transform.impl;

import com.pdomingo.pipeline.transform.Transformer;

/**
 * Clase de adaptacion identidad
 * @param <T> tipo del que convertir
 * @param <U> tipo al que convertir
 */
public class IdentityTransformer<T, U> implements Transformer<T, U> {
    /**
     * Transforma el parametro de tipo T en uno de tipo U
     * @param t parametro de entrada tipo T
     * @return objeto de tipo U
     */
    @Override
    public U transform(T t) {
        return (U) t;
    }
}