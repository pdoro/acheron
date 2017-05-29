package com.pdomingo.zmq;

import lombok.extern.slf4j.Slf4j;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Date;

@Slf4j
public class Heartbeat {

    public static long now() {
        return System.currentTimeMillis();
    }

    public Heartbeat() {
        this(HEARTBEAT_INTERVAL);
    }
    public Heartbeat(long heartbeat_interval) {
        this.heartbeat_interval = heartbeat_interval;
        this.liveness = MAX_LIVENESS;
        this.dead = false;
        this.lastHeartbeatRecv = now();
        this.nextHeartbeatAt = now();
    }

    /////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////// TRACK HEARTBEATS FROM SELF TO REMOTE ENDPOINT /////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////

    // Msec when the local endpoint should heartbeat the remote one
    private long nextHeartbeatAt;
    private final long heartbeat_interval;

    public boolean isTimeToBeat() {
        return now() >= nextHeartbeatAt && !dead;
    }

    public long remainingTimeToBeat() {
        long now = now();
        long diff = nextHeartbeatAt - now;
        return diff > 0 ? diff : 0; // Avoid negative diff so poll doesn't wait infinitely
    }

    public ZMsg beatToEndpoint(String address) {
        ZMsg msg = new ZMsg();
        msg.add(address);
        msg.add(new ZFrame(ZMQ.MESSAGE_SEPARATOR));
        msg.add(CMD.HEARTBEAT.newFrame());
        updateSelfBeat();
        return msg;
    }

    public ZMsg beatToEndpoint() {
        ZMsg beat = new ZMsg();
        beat.add(new ZFrame(ZMQ.MESSAGE_SEPARATOR));
        beat.add("normalize");
        beat.add(CMD.WORKER.newFrame());
        beat.add(CMD.HEARTBEAT.newFrame());
        updateSelfBeat();
        return beat;
    }

    public void updateSelfBeat() {
        nextHeartbeatAt = now() + heartbeat_interval;
        log.trace("updateSelfBeat - Next HB expected at {}", new Date(nextHeartbeatAt));
    }

    /////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////// TRACK HEARTBEATS FROM REMOTE ENDPOINT TO SELF /////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////

    public static final int MAX_LIVENESS = 3;
    public static final int MAX_RECONNECT_DELAY = 32000; // in msecs
    public static final int HEARTBEAT_INTERVAL = 7000;   // in msecs

    private int         liveness; // remaining attempts to decide if other end is dead
    private long  reconnectDelay = 1000; // in msecs
    private boolean         dead;

    private long lastHeartbeatRecv;

    /**
     * Represent a positive heartbeat from the other end
     */
    public void beatFromEndpoint() {
        liveness = MAX_LIVENESS; // reset remaining attempts
        lastHeartbeatRecv = now();
    }

    public boolean remoteHeartbeatExpired() {
        return now() > lastHeartbeatRecv + heartbeat_interval;
    }

    /**
     * Represent a failed heartbeat from the other end
     */
    public void failFromEndpoint() {
        if(isAlive())
            liveness -= 1;
    }

    public boolean seemsDead() {
        return liveness <= 0;
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
            liveness = MAX_LIVENESS;      // restart liveness counter
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public long getReconnectDelay() {
        return reconnectDelay;
    }
}