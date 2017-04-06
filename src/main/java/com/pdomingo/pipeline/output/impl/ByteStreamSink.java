package com.pdomingo.pipeline.output.impl;

import com.pdomingo.pipeline.output.Sink;
import com.pdomingo.pipeline.transform.Transformer;
import com.pdomingo.pipeline.transform.impl.TransformerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class ByteStreamSink<T> implements Sink<T> {

    private final OutputStream os;
    private final Transformer<T, byte[]> transformer;

    public ByteStreamSink(OutputStream os, Transformer<T, byte[]> transformer) {

        this.transformer = transformer;
        if(os instanceof BufferedOutputStream)
            this.os = os;
        else
            this.os = new BufferedOutputStream(os); // ALWAYS buffer!
    }

    public ByteStreamSink(OutputStream os) {
        this(os, TransformerFactory.identity());
    }

    @Override
    public void write(T item) {
        try {
            os.write(transformer.transform(item));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
