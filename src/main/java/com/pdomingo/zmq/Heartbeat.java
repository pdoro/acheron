package com.pdomingo.zmq;

import org.zeromq.ZMQ;

/**
 *
 */
public class Heartbeat {

    public final static int LIVENESS = 3;
    public final static int TIMEOUT = 1000;    // in msecs
    public final static int MAX_RECONNECT_DELAY = 32000; // in msecs

    private int         liveness; // remaining attempts to decide if other end is dead
    private long  reconnectDelay; // in msecs
    private boolean         dead;

    private long   lastHeartbeat;

    /**
     * Represent a positive heartbeat from the other end
     */
    public void beat() {
        liveness = LIVENESS; // reset remaining attempts
    }

    /**
     * Represent a failed heartbeat from the other end
     */
    public void fail() {
        if(isAlive())
            liveness -= 1;
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

    public boolean timeBeat() {
        long now = System.currentTimeMillis();
        if(now >= lastHeartbeat + TIMEOUT && !dead) {
            lastHeartbeat = now;
            return true;
        }
        return false;
    }

    public void sendHeartbeat(ZMQ.Socket destination) {

    }

    public long getReconnectDelay() {
        return reconnectDelay;
    }
}