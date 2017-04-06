package com.pdomingo.zmq;

import lombok.extern.slf4j.Slf4j;
import org.zeromq.ZMQ;
import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

/**
 *
 */
@Slf4j
public class Heartbeat {

    public final static int LIVENESS = 3;
    public final static int TIMEOUT = 2500;    // in msecs
    public final static int MAX_RECONNECT_DELAY = 32000; // in msecs
    public final static int HEARTBEAT_INTERVAL = 2400;    // in msecs

    private int         liveness; // remaining attempts to decide if other end is dead
    private long  reconnectDelay = 1000; // in msecs
    private boolean         dead;

    private long lastHeartbeatSent;
    private long lastHeartbeatRecv;

    /**
     * Represent a positive heartbeat from the other end
     */
    public void beat() {
        liveness = LIVENESS; // reset remaining attempts
        lastHeartbeatRecv = now();
    }

    /**
     * Represent a failed heartbeat from the other end
     */
    public void fail() {
        if(isAlive())
            liveness -= 1;
    }

    public void reset() {
        liveness = LIVENESS; // reset remaining attempts
        lastHeartbeatRecv = System.currentTimeMillis();
    }

    public boolean seemsDead() {
        return liveness == 0;
    }

    /**
     *@return if the other end represented by this heartbeat is still alive
     */
    public boolean isAlive() {
        return !dead;
    }

    /**
     *
     * @return if the other end represented by this heartbeat is still alive
     */
    public boolean isDead() {
        return dead;
    }

    public void reconnectDelay() {

        if(reconnectDelay >= MAX_RECONNECT_DELAY) {
            dead = true; // endpoint is definitely dead
            return;
        }

        try {
            Thread.sleep(reconnectDelay); // give time for the endpoint to resurrect
            reconnectDelay *= 2;           // exponential backoff
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public boolean timeToBeat() {
        long now = now();
        if((now - lastHeartbeatSent) > HEARTBEAT_INTERVAL && !dead) {
            lastHeartbeatSent = now;
            return true;
        }
        return false;
    }

    public ZMsg sendHeartbeat(String address) {
        ZMsg msg = new ZMsg();
        msg.add(address);
        msg.add(new ZFrame(ZMQ.MESSAGE_SEPARATOR));
        msg.add(CMD.HEARTBEAT.newFrame());
        lastHeartbeatSent = now();
        return msg;
    }

    public ZMsg sendHeartbeat() {
        ZMsg beat = new ZMsg();
        beat.add(new ZFrame(ZMQ.MESSAGE_SEPARATOR));
        beat.add("normalize");
        beat.add(CMD.WORKER.newFrame());
        beat.add(CMD.HEARTBEAT.newFrame());
        lastHeartbeatSent = now();
        return beat;
    }


    public long getReconnectDelay() {
        return reconnectDelay;
    }

    public boolean heartbeatExpired() {
        return now() > lastHeartbeatRecv;
    }

    private long now() {
        return System.currentTimeMillis();
    }
}