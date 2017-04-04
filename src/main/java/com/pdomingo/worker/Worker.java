package com.pdomingo.worker;

import com.pdomingo.zmq.CMD;
import com.pdomingo.zmq.Heartbeat;
import org.zeromq.*;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Worker {

    /*--------------------------- Attributes ---------------------------*/

    private ZContext ctx;
    private ZMQ.Socket worker;
    private ZPoller poller;

    private String endpoint;
    private String service;

    private Heartbeat heart; // broker's heart

    private boolean verbose;

	/*--------------------------- Constructor ---------------------------*/

    public Worker() {
        ctx = new ZContext();
        poller = new ZPoller(ctx.createSelector());
        poller.register(worker, ZPoller.IN);
    }

	/*--------------------------- Private methods ---------------------------*/

    private void reconnectToEndpoint() {

        log.trace("Attempting to reconnect broker");

        if (worker != null) {
            ctx.destroySocket(worker);
            log.trace("Destroyed previous socket");
        }

        worker = ctx.createSocket(ZMQ.DEALER);
        worker.connect(endpoint);

        log.trace("Successful connection to broker at {}", endpoint);

        // Register service with broker
        sendToBroker(CMD.READY, service, null);

        heart.beat();
    }

    private void sendToBroker(CMD command, String option, ZMsg msg) {

        msg = msg != null ? msg.duplicate() : new ZMsg();

        // Stack protocol envelope to start of message
        if (option != null)
            msg.addFirst(new ZFrame(option));

        msg.addFirst(command.newFrame());
        msg.addFirst(CMD.WORKER.newFrame());
        msg.addFirst(new ZFrame(ZMQ.MESSAGE_SEPARATOR));

        if (verbose) {
            System.out.println("Sending" + command + "to broker");
            msg.dump();
        }

        msg.send(worker);
    }

    public ZMsg receive() {

        while (!Thread.currentThread().isInterrupted()) {

            if (poller.poll(Heartbeat.TIMEOUT) == -1)
                break; // Interrupted

            if (poller.isReadable(worker)) {

                ZMsg msg = ZMsg.recvMsg(worker);
                if (msg == null)
                    break;

                if (verbose) {
                    System.out.println("Received message from broker: \n");
                    msg.dump();
                }

                heart.beat(); // use received message as a signal of alive endpoint

                // Don't try to handle errors, just assert noisily
                assert (msg.size() >= 3);

                ZFrame empty = msg.pop();
                assert (empty.getData().length == 0);
                empty.destroy();

                CMD command = CMD.resolveCommand(msg.pop());
                processCommand(command, empty);

            } else { // heartbeat passed but we received no request
                heart.fail();

                if (heart.seemsDead()) {

                    log.trace("Endpoint seems dead. Waiting {} msecs until reconnection", heart.getReconnectDelay());
                    heart.reconnectDelay();

                    if (heart.isDead()) {
                        log.trace("Endpoint declared dead, destroying worker");
                        destroyWorker();
                    }
                    else
                        reconnectToEndpoint();
                }
            }

            if (heart.timeBeat())
                heart.sendHeartbeat(worker);
        }
        return null;
    }

    public void destroyWorker() {

    }

    public void processCommand(CMD command, ZFrame payload) {

        switch (command) {

            case REQUEST:
                // process request
                break;

            case HEARTBEAT:
                // Good to know!
                // The message received was considered a beat
                break;

            case DISCONNECT:
                log.warn("Received disconnect command");
                reconnectToEndpoint();
                break;

            case READY:
            case CLIENT:
            case WORKER:
            case REPLY:
                log.warn("Invalid command {}", command.name());
                break;

            default:
        }
    }
}
