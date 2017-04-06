package com.pdomingo.worker;

import com.pdomingo.zmq.CMD;
import com.pdomingo.zmq.Heartbeat;
import com.pdomingo.zmq.ZHelper;
import org.zeromq.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Worker {

    /*--------------------------- Attributes ---------------------------*/

    private ZContext ctx;
    private ZMQ.Socket socket;
    private ZPoller poller;

    private String endpoint;
    private String service = "normalize";
    private final String address;

    private Heartbeat heart; // broker's heart

    private boolean verbose;

	/*--------------------------- Constructor ---------------------------*/

    public Worker(String endpoint, int n) {
        this.endpoint = endpoint;
        this.address = "WORKER0" + n;
        ctx = new ZContext();
        heart = new Heartbeat();

        log.info("[{}] Worker started", address);
        reconnectToEndpoint();

        while(true) {
            receive();
        }
    }

	/*--------------------------- Private methods ---------------------------*/

    private void reconnectToEndpoint() {

        log.info("[{}] Attempting to reconnect broker", address);

        if (socket != null) {
            ctx.destroySocket(socket);
            log.warn("[{}] Destroyed previous socket", address);
        }

        socket = ctx.createSocket(ZMQ.DEALER);
        socket.setIdentity(address.getBytes());
        socket.connect(endpoint);

        poller = new ZPoller(ctx.createSelector());
        poller.register(socket, ZPoller.IN);


        log.info("[{}] Successful connection to broker at {}", address, endpoint);

        // Register service with broker
        sendToBroker(CMD.READY, service, null);

        heart.reset();
    }

    private void sendToBroker(CMD command, String option, ZMsg msg) {

        log.trace("[{}] Send CMD:'{}' option='{}'", address, command, option);

        msg = msg != null ? msg.duplicate() : new ZMsg();

        msg.add(new ZFrame(ZMQ.MESSAGE_SEPARATOR));

        // Stack protocol envelope to start of message
        if (option != null)
            msg.add(new ZFrame(option));

        msg.add(CMD.WORKER.newFrame());
        msg.add(command.newFrame());

        if(log.isTraceEnabled())
            log.trace("[{}] Sending READY command to broker {}", address, ZHelper.dump(msg));

        msg.send(socket);
    }

    public ZMsg receive() {

        while (!Thread.currentThread().isInterrupted()) {

            log.debug("[{}] Poll started", address);
            if (poller.poll(Heartbeat.TIMEOUT) == -1) {
                log.error("[{}] Heartbeat interrupted prematurely", address);
                break; // Interrupted
            }

            if (poller.isReadable(socket)) {

                ZMsg msg = ZMsg.recvMsg(socket);

                if(log.isTraceEnabled())
                    log.trace("[{}] Message received from broker {}", address, ZHelper.dump(msg));

                if (msg == null) {
                    log.error("[{}] Message receive interrupted prematurely", address);
                    break;
                }

                heart.beat(); // use received message as a signal of alive endpoint

                // Don't try to handle errors, just assert noisily
                assert (msg.size() >= 3); // TODO modificar por log y continue

                ZFrame empty = msg.pop();
                assert (empty.getData().length == 0); // TODO modificar por log y continue
                empty.destroy();

                CMD command = CMD.resolveCommand(msg.pop());
                processCommand(command, empty);

            } else { // heartbeat passed but we received no request
                log.warn("[{}] Heartbeat failed", address);
                heart.fail();

                if (heart.seemsDead()) {

                    log.warn("[{}] Endpoint seems dead. Waiting {} msecs until reconnection", address, heart.getReconnectDelay());
                    heart.reconnectDelay();

                    if (heart.isDead()) {
                        log.error("[{}] Endpoint declared dead, destroying socket", address);
                        destroyWorker();
                    }
                    else
                        reconnectToEndpoint();
                }
            }

            if (heart.timeToBeat())
                heart.sendHeartbeat().send(socket);
        }

        return null;
    }

    private void destroyWorker() {
        ctx.destroySocket(socket);
        ctx.destroy();
        log.info("[{}] Worker destroyed", address);
    }

    private void processCommand(CMD command, ZFrame payload) {

        switch (command) {

            case REQUEST:
                log.trace("[{}] - {}", command, payload.toString());
                // process request
                break;

            case HEARTBEAT:
                log.info("[{}] Received heartbeat from endpoint", address);
                // Good to know!
                // The message received was considered a beat
                break;

            case DISCONNECT:
                log.warn("[{}] Received disconnect command", address);
                reconnectToEndpoint();
                break;

            default:
                log.warn("[{}] Invalid command {}", address, command.name());
        }
    }
}