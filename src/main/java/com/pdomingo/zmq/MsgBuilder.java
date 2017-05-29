package com.pdomingo.zmq;

import org.zeromq.ZFrame;
import org.zeromq.ZMsg;
import zmq.Msg;

import java.util.Arrays;

/**
 * Created by pdomingo on 28/05/17.
 */
public class MsgBuilder {

    private ZMsg msg = new ZMsg();

    public static MsgBuilder start() {
        return new MsgBuilder();
    }

    public MsgBuilder add(String text) {
        msg.add(text);
        return this;
    }

    public MsgBuilder addAll(String... texts) {
        for (String txt : texts) {
            add(txt);
        }
        return this;
    }

    public MsgBuilder add(ZFrame frame) {
        msg.add(frame);
        return this;
    }

    public MsgBuilder addAll(ZFrame... frames) {
        msg.addAll(Arrays.asList(frames));
        return this;
    }

    public MsgBuilder add(byte[] stream) {
        msg.add(stream);
        return this;
    }

    public MsgBuilder add(byte[]... streams) {
        for (byte[] stream : streams) {
            add(stream);
        }
        return this;
    }

    public MsgBuilder add(CMD command) {
        msg.add(command.newFrame());
        return this;
    }

    public MsgBuilder add(CMD... commands) {
        for (CMD command : commands) {
            add(command);
        }
        return this;
    }

    public MsgBuilder add(ZMsg msg) {
        this.msg.addAll(msg);
        return this;
    }

    public ZMsg build() {
        return msg;
    }
}
