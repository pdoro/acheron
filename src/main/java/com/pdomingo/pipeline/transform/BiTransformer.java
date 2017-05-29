package com.pdomingo.pipeline.transform;

/**
 * Created by pdomingo on 28/05/17.
 */
public interface BiTransformer<T,U> {

    U forwardTransform(T t);
    T backwardTransform(U u);
}
