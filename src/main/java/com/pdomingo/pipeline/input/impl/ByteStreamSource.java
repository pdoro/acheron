package com.pdomingo.pipeline.input.impl;

import com.pdomingo.pipeline.input.Source;
import com.pdomingo.pipeline.transform.Transformer;
import com.pdomingo.pipeline.transform.impl.TransformerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ByteStreamSource<T> implements Source<T> {

    private final InputStream is;
    private final Transformer<byte[], T> transformer;

    private final int BUFFER_SIZE = 4096;
    private byte[] buffer = new byte[BUFFER_SIZE];
    private int offset = 0;

    public ByteStreamSource(InputStream is, Transformer<byte[], T> transformer) {
        this.transformer = transformer;
        if(is instanceof BufferedInputStream)
            this.is = is;
        else
            this.is = new BufferedInputStream(is); // ALWAYS buffer!
    }

    public ByteStreamSource(InputStream is) {
        this(is, TransformerFactory.identity());
    }

    @Override
    public T read() {
        try {
            int bytesRead = is.read(buffer, offset, BUFFER_SIZE);
            offset += bytesRead;
            return bytesRead > 0 ? transformer.transform(buffer) : null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
