package com.pdomingo.client;

import com.esotericsoftware.kryo.Serializer;
import com.pdomingo.zmq.CMD;
import com.pdomingo.zmq.ZHelper;
import lombok.extern.slf4j.Slf4j;
import org.zeromq.*;

import java.io.Closeable;

@Slf4j
public class Client<T> implements Closeable {

    /*--------------------------- Attributes ---------------------------*/

    private final ZContext ctx;
    private ZMQ.Socket socket;
    private ZPoller poller;

    // Timeout until request retry
    private int timeout = 2500; // msecs
    // Max number of request until the client gives up
    private int retries = 3;
    // Type communication between client and service
    private final boolean async;
    // Number of messages per batch. Non applicable for sync
    private int batchSize;

    private final String endpoint;
    private String requestedService;
    private final Serializer<T> serializer;
    private final ResponseHandler<T> asynHandler;

    /*--------------------------- Constructor ---------------------------*/

    private Client(Builder<T> builder) {

        ctx = new ZContext();

        endpoint = builder.endpoint;
        async = builder.async;
        batchSize = async ? 1 : builder.batchSize;
        timeout = builder.timeout;
        retries = builder.retries;
        serializer = builder.serializer;
        asynHandler = builder.handler;

        reconnectToEndpoint();
    }

    /*--------------------------- Private methods ---------------------------*/

    private void reconnectToEndpoint() {
        log.trace("Attempting to reconnect broker");

        if(socket != null) {
            ctx.destroySocket(socket);
            log.trace("Destroyed previous socket");
        }

        int socketType = async ? ZMQ.DEALER : ZMQ.REQ;
        socket = ctx.createSocket(socketType);
        socket.connect(endpoint);

        log.trace("Successful connection to broker at {}", endpoint);
    }

    public void send(String payload) {

        ZMsg msg = buildRequest(payload, requestedService);

        while(retries > 0) {

            // Duplicate in case there's no response
            // so the original data is not lost
            msg.duplicate().send(socket);

            if(poller.poll(timeout) == -1)
                break; // Interrupted

            if(poller.writable(socket)) {

                ZHelper.dump(socket);
                //ZMsg response = ZMsg.recvMsg(socket);

            } else {
                poller.unregister(socket);
                if(--retries == 0) {
                    log.error("Retry limit reached");
                    break;
                }
                reconnectToEndpoint();
            }
        }

        retries = 3;
    }

    private static final ZMsg buildRequest(String payload, String service) {
        ZMsg msg = new ZMsg();
        msg.add(new ZFrame(ZMQ.MESSAGE_SEPARATOR)); // Frame 0 - empty (REQ compatibility) //
        msg.add(CMD.CLIENT.newFrame());             // Frame 1 - MDP Command               //
        msg.add(service);                           // Frame 2 - Service request           //
        msg.add(payload);                           // Frame 3 - Request payload           //
        return msg;
    }

    @Override
    public void close() {
        ctx.destroySocket(socket);
        ctx.destroy();
        log.info("Client destroyed");
    }

    public interface ResponseHandler<T> {
        void handle(T response);
    }

    public static class Builder<T> {

        private String endpoint;
        private boolean async = true;
        private int batchSize = 10000;
        private int timeout = 2500; // msecs
        private int retries = 3;
        private Serializer<T> serializer;
        private ResponseHandler<T> handler;

        public static <T> Builder<T> start() { return new Builder<>(); }

        public Builder<T> connectTo(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder<T> serializeUsing(Serializer<T> serializer) {
            this.serializer = serializer;
            return this;
        }

        public Builder<T> async(boolean async) {
            this.async = async;
            return this;
        }

        public Builder<T> batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder<T> timeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder<T> retries(int retries) {
            this.retries = retries;
            return this;
        }

        public Builder<T> responseHandler(ResponseHandler<T> handler) {
            this.handler = handler;
            return this;
        }

        public Client<T> build() {
            return new Client<>(this);
        }
    }
}