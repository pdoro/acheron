package com.pdomingo.pipeline.transform.impl;

import com.pdomingo.pipeline.transform.Transformer;
import java.util.HashMap;
import java.util.Map;

/**
 * Permite generar transformadores genericos para cualquier tipo de dato
 *
 * @author pdomingo
 */
public class TransformerFactory<T, U> {

    /* Instancia generica del transformador indentidad */
    private final IdentityTransformer<T, U> IDENTITY_ADAPTER = new IdentityTransformer<>();
    /* Conjunto de transformers que la fabrica puede crear. Deben ser previamente registrados usando register() */
    private final Map<String, Transformer<T, U>> availableTransformers = new HashMap<>();

    /**
     * Crea (recupera) un adaptador especifico segun el alias de entrada
     * @param alias nombre del transformador a crear
     * @return trans
     */
    public Transformer<T, U> lookup(String alias) {
        return availableTransformers.getOrDefault(alias, IDENTITY_ADAPTER);
    }

    /**
     * Registra un nuevo transformador junto al alias por el que se solicitara
     * @param alias nombre del transformador a registrar
     * @param transformer implementacion a registrar
     * @return this
     */
    public TransformerFactory<T, U> register(String alias, Transformer<T, U> transformer) {
        availableTransformers.put(alias, transformer);
        return this;
    }

    /**
     * Retorna el transformador indentidad para los tipos genÃ©ricos {@link T} y {@link U}
     * @param <T> tipo de entrada
     * @param <U> tipo de salida
     * @return transformador T -> U
     */
    public static <T, U> Transformer<T, U> identity() {
        return new IdentityTransformer<>();
    }

    /**
     * Compone dos transformadores de tipos compatibles en un unico transformados
     * La idea es componer:
     * A to B && B to C == A to C
     *
     * @param t1 transformador A -> B
     * @param t2 transformador B -> C
     * @param <A> tipo de objeto A
     * @param <B> tipo de objeto B
     * @param <C> tipo de objeto C
     * @return transformador compuesto
     */
    public static <A,B,C> Transformer<A, C> compose(final Transformer<A, B> t1, final Transformer<B, C> t2) {
        return new Transformer<A, C>() {
            @Override
            public C transform(A a) {
                return t2.transform(t1.transform(a));
            }
        };
    }
}
