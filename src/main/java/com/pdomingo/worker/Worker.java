package com.pdomingo.worker;

import com.pdomingo.client.BatchRequest;
import com.pdomingo.pipeline.transform.Serializer;
import com.pdomingo.zmq.CMD;
import com.pdomingo.zmq.Heartbeat;
import com.pdomingo.zmq.ZHelper;
import org.zeromq.ZFrame;
import org.zeromq.*;
import lombok.extern.slf4j.Slf4j;
import java.util.function.Function;

/**
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 * @param <T> type
 */
@Slf4j
public class Worker<T> {

    /*--------------------------- Attributes ---------------------------*/

    private ZContext ctx;
    private ZMQ.Socket socket;
    private ZPoller poller;

    private String endpoint;
    private final String service;
    private final String address;

    private Heartbeat heart; // broker's heartbeat communication
    private boolean continueRunning;

    private int replyNo = 0;
    private final Serializer<T> serializer;
    private final Function<T,T> processor;

	/*--------------------------- Constructor ---------------------------*/

    public Worker(Builder<T> builder) {
        this.endpoint = builder.endpoint;
        this.service = builder.service;
        this.address = builder.address;
        this.ctx = new ZContext();
        this.heart = new Heartbeat();
        this.serializer = builder.serializer;
        this.processor = builder.processor;
        this.continueRunning = true;
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

        log.info("[{}] Connection to broker at {}", address, endpoint);

        // Register service with broker
        sendToBroker(CMD.READY, service);
    }

    private void sendToBroker(CMD command, String service, String... payload) {

        log.trace("[{}] Send CMD:'{}' option='{}'", address, command, service);

        ZMsg msg = new ZMsg();

        msg.add(ZMQ.MESSAGE_SEPARATOR);
        msg.add(service);
        msg.add(CMD.WORKER.newFrame());
        msg.add(command.newFrame());

        for(String payloadPart : payload)
            msg.add(payloadPart);

        log.trace("[{}] Sending READY command to broker {}", address, ZHelper.dump(msg, log.isTraceEnabled()));

        msg.send(socket);
    }

    private void sendToBroker(CMD command, String service, ZMsg payload) {
        log.trace("[{}] Send CMD:'{}' option='{}'", address, command, service);

        ZMsg msg = new ZMsg();

        msg.add(ZMQ.MESSAGE_SEPARATOR);
        msg.add(service);
        msg.add(CMD.WORKER.newFrame());
        msg.add(command.newFrame());
        msg.addAll(payload);

        log.trace("[{}] Sending {} command to broker {}", address, command, ZHelper.dump(msg, log.isTraceEnabled()));

        msg.send(socket);
    }

    public void receiveIndefinitely() {

        while (!Thread.currentThread().isInterrupted() && continueRunning) {

            log.debug("[{}] Poll started - Timeout {}", address, Heartbeat.HEARTBEAT_INTERVAL);
            if (poller.poll(heart.remainingTimeToBeat()) == -1) {
                log.error("[{}] Heartbeat interrupted prematurely", address);
                break; // Interrupted
            }

            if (poller.isReadable(socket)) {
                ZMsg msg = ZMsg.recvMsg(socket);
                handleIncomingMessage(msg);

            } else {

                if(heart.remoteHeartbeatExpired()) {

                    // heartbeat interval passed but we received no request
                    log.warn("[{}] Expected heartbeat from broker failed", address);
                    heart.failFromEndpoint();

                    if (heart.seemsDead()) {

                        log.warn("[{}] Endpoint seems dead. Waiting {} msecs until reconnection", address, heart.getReconnectDelay());
                        heart.reconnectDelay();

                        if (heart.isDead()) {
                            log.error("[{}] Endpoint declared dead, destroying socket", address);
                            destroyWorker();
                        } else
                            reconnectToEndpoint();
                    }
                }
            }

            if (heart.isTimeToBeat()) {
                heart.beatToEndpoint(service).send(socket);
                log.debug("[{}] Sent heartbeat to endpoint {}", address, endpoint);
            }
        }

        log.info("Worker finishing...");
        destroyWorker();
        log.info("Worker successsfully terminated");
    }

    private void handleIncomingMessage(ZMsg msg) {

        if (msg == null) {
            log.error("[{}] Message receive interrupted prematurely", address);
            return;
        }

        log.trace("[{}] Message received from broker {}", address, ZHelper.dump(msg, log.isTraceEnabled()));

        heart.beatFromEndpoint(); // use received message as a signal of alive endpoint

        // Don't try to handle errors, just assert noisily
        assert (msg.size() >= 2); // TODO modificar por log y continue
        ZFrame empty = msg.pop();
        assert (empty.getData().length == 0); // TODO modificar por log y continue
        empty.destroy();

        CMD command = CMD.resolveCommand(msg.pop());
        processCommand(command, msg);
    }

    private void destroyWorker() {
        ctx.destroySocket(socket);
        ctx.destroy();
        log.info("[{}] Worker destroyed", address);
    }

    private void processCommand(CMD command, ZMsg message) {

        switch (command) {

            case REQUEST:

                ZFrame senderAddress = message.pop();

                BatchRequest<T> requests = BatchRequest.fromMsg(serializer, message);

                for (T payload : requests)
                    processor.apply(payload);

                ////////////////////////////////////////////////////
                message.destroy();

                ZMsg response = requests.toMsg(serializer, CMD.WORKER, CMD.REPLY, service, senderAddress);
                log.trace("[{}] OUTPUT {} - {}", address, command, ZHelper.dump(response, log.isTraceEnabled()));

                response.send(socket);

                if(replyNo++ % 1000 == 0)
                    log.trace("[{}] Send reply no {}", address, replyNo++);

                break;

            case HEARTBEAT:
                log.debug("[{}] Received heartbeat from endpoint", address);
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

    public void start() {
        log.info("[{}] Worker started", address);
        reconnectToEndpoint();
        receiveIndefinitely();
    }

    public void terminate() {
        continueRunning = false;
        log.info("Worker termination requested");
    }

    /* -------------------------------------------------------------------------- */

    public static class Builder<T> {

        private String endpoint;
        private String service;
        private String address = ZHelper.randomId();
        private Serializer<T> serializer;
        private Function<T,T> processor;

        public static <T> Builder<T> start() {
            return new Builder<>();
        }

        public Builder<T> connectTo(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder<T> address(String address) {
            this.address = address;
            return this;
        }

        public Builder<T> serializeUsing(Serializer<T> serializer) {
            this.serializer = serializer;
            return this;
        }

        public Builder<T> forService(String service) {
            this.service = service;
            return this;
        }

        public Builder<T> useFunction(Function<T,T> processor) {
            this.processor = processor;
            return this;
        }

        public Worker<T> build() {
            return new Worker<>(this);
        }
    }
}