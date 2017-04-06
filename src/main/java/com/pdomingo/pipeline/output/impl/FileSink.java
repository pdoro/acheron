package com.pdomingo.pipeline.output.impl;

import com.pdomingo.pipeline.output.Sink;
import com.pdomingo.pipeline.transform.Transformer;
import com.pdomingo.pipeline.transform.impl.TransformerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileSink<T> implements Sink<T> {

    private BufferedWriter bufferedWriter;
    private final Transformer<T, String> transformer;

    public FileSink(Path path, Transformer<T, String> transformer) {

        Charset charset = Charset.defaultCharset();
        this.transformer = transformer;

        try {
            bufferedWriter = Files.newBufferedWriter(path, charset);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public FileSink(Path path) {
        this(path, TransformerFactory.identity());
    }

    @Override
    public void write(T item) {
        try {
            bufferedWriter.write(transformer.transform(item));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
