package com.pdomingo.zmq;

import org.zeromq.ZFrame;
import org.zeromq.ZMQ;

import java.util.Arrays;

/**
 *
 */
public enum CMD {

    CLIENT,
    WORKER,

    READY,
    HEARTBEAT,
    REQUEST,
    REPLY,
    DISCONNECT,

    INVALID;

    private final byte[] data;
    private static final CMD[] values = values(); // cache

    CMD(String value) {
        this.data = value.getBytes(ZMQ.CHARSET);
    }

    CMD() { //watch for ints > 255, will be truncated
        byte b = (byte) (ordinal() & 0xFF);
        this.data = new byte[] { b };
    }

    public ZFrame newFrame () {
        //return new ZFrame(this.name()); // debug purposes
        return new ZFrame(data);
    }

    public boolean frameEquals (ZFrame frame) {
        //return Arrays.equals(name().getBytes(), frame.getData()); // debug purposes
        return Arrays.equals(data, frame.getData());
    }

    public static CMD resolveCommand(ZFrame frame) {
        for(CMD command : values)
            if(command.frameEquals(frame))
                return command;

        return INVALID;
    }
}