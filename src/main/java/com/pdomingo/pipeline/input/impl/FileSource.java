package com.pdomingo.pipeline.input.impl;

import com.pdomingo.pipeline.input.Source;
import com.pdomingo.pipeline.transform.Transformer;
import com.pdomingo.pipeline.transform.impl.TransformerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 *
 * @param <T>
 */
public class FileSource<T> implements Source<T> {

    private BufferedReader bufferedReader;
    private final Transformer<String, T> transformer;

    public FileSource(Path path, Transformer<String, T> transformer) {

        Charset charset = Charset.defaultCharset();
        this.transformer = transformer;

        try {
            bufferedReader = Files.newBufferedReader(path, charset);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public FileSource(Path path) {
        this(path, TransformerFactory.identity());
    }

    @Override
    public T read() {
        try {
            return transformer.transform(bufferedReader.readLine());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
