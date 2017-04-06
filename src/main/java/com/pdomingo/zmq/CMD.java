package com.pdomingo.zmq;

import org.zeromq.ZFrame;
import org.zeromq.ZMQ;

import java.util.Arrays;

/**
 *
 */
public enum CMD {

    CLIENT(1),
    WORKER(2),

    READY(3),
    HEARTBEAT(3),
    REQUEST(4),
    REPLY(5),
    DISCONNECT(6),

    INVALID(7);

    private final byte[] data;

    CMD(String value) {
        this.data = value.getBytes(ZMQ.CHARSET);
    }

    CMD(int value) { //watch for ints > 255, will be truncated
        byte b = (byte) (value & 0xFF);
        this.data = new byte[] { b };
    }

    public ZFrame newFrame () {
        return new ZFrame(this.name());
        //return new ZFrame(data);
    }

    public boolean frameEquals (ZFrame frame) {
        return Arrays.equals(name().getBytes(), frame.getData());
        //return Arrays.equals(data, frame.getData());
    }

    public static CMD resolveCommand(ZFrame frame) {
        for(CMD command : CMD.values())
            if(command.frameEquals(frame))
                return command;

        return INVALID;
    }
}