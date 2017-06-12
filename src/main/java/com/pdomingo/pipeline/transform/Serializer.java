package com.pdomingo.pipeline.transform;

/**
 * Created by pdomingo on 28/05/17.
 */
public interface Serializer<T> {

    /**
     * Transform the object into a byte array
     * @param t object to serialize
     * @return binary representation of the object
     */
    byte[] write(T t);

    /**
     * Reads the binary representation of an object
     * and return the java object deserialized
     * @param b array of bytes
     * @return the object
     */
    T read(byte[] b);
}
