package com.pdomingo.pipeline.input.impl;

import com.pdomingo.pipeline.input.Source;

import java.util.Iterator;

public class IterableSource<T> implements Source<T> {

    private Iterator<T> iterator;

    public IterableSource(Iterator<T> iterator) {
        this.iterator = iterator;
    }

    public IterableSource(Iterable<T> iterable) {
        this(iterable.iterator());
    }

    @Override
    public T read() {
        return iterator.hasNext() ? iterator.next() : null;
    }
}