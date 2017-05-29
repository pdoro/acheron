package com.pdomingo.client;

import com.google.common.base.Charsets;
import com.pdomingo.pipeline.transform.BiTransformer;
import com.pdomingo.zmq.CMD;
import com.pdomingo.zmq.ZHelper;
import lombok.extern.slf4j.Slf4j;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import org.zeromq.ZPoller;

import java.io.Closeable;
import java.util.Collection;

/**
 * Pieza del servicio distribuido que actua como cliente del servicio.
 * Para ello se conecta con un nodo {@link com.pdomingo.broker.Broker} y
 * manda mensajes de tipo {@link BatchRequest}.
 *
 * Almacena mensajes en lotes para despues escribirlos de golpe y la vuelta
 * llama al metodo de callback proporcionado como {@link #asyncHandler}
 *
 * Los parametros de configuración del cliente son los siguientes:
 * <ul>
 *     <li>Async: define si se trata de un cliente sincrono (escribe
 *     mensaje y no se desbloquea hasta recibir una respuesa) o asincrono
 *     (puede escribir y esperar respuestas en el orden que quiera y tantas
 *     veces como desee)</li>
 *     <li>Batch size: tamaÃ±o de los lotes, define cuantos objetos contiene
 *     cada lote</li>
 *     <li>Write Burst: numero de lotes a escribir de golpe</li>
 *     <li>Timeout: tiempo de gracia que se da desde que un mensaje se envia
 *     hasta que se espera recibir su respuesta</li>
 *     <li>Retries: numero de reintentos en caso de que falle la respuesta de
 *     un mensaje</li>
 * </ul>
 *
 * @param <T>
 */
@Slf4j
public class Client<T> implements Closeable {

    /*--------------------------- Attributes ---------------------------*/

    private final ZContext ctx;
    private ZMQ.Socket socket;
    private ZPoller poller;

    // Timeout until request retry
    private final int timeout; // msecs
    // Max number of request until the client gives up
    private int retries;
    // Type of communication between client and service
    private final boolean async;
    // Number of messages per batch. Non applicable for sync
    private final int batchSize;
    // Messages to write until we await for responses
    private final int writeBurst;

    private final String address;
    private final String endpoint;
    private String requestedService = "normalize";
    private final BiTransformer<T, byte[]> serializer;
    private final ResponseHandler<T> asyncHandler;

    private int writtenRequests;
    private BatchRequest<T> currentBatch;
    private ReorderBuffer<T> reorderBuffer;

    /*--------------------------- Constructor ---------------------------*/

    private Client(Builder<T> builder) {

        ctx = new ZContext();

        endpoint = builder.endpoint;
        async = builder.async;
        batchSize = async ? builder.batchSize : 1;
        writeBurst = async ? builder.writeBurst : 1;
        timeout = builder.timeout;
        retries = builder.retries;
        serializer = builder.serializer;
        asyncHandler = builder.handler;
        address = "Client01";//ZHelper.randomId();

        currentBatch = new BatchRequest<>(1, batchSize);
        reorderBuffer = new ReorderBuffer<>(writeBurst);

        reconnectToEndpoint();

        log.debug("Client started");
    }

    /*--------------------------- Private methods ---------------------------*/

    private void reconnectToEndpoint() {
        log.trace("Attempting to reconnect broker");

        if (socket != null) {
            ctx.destroySocket(socket);
            log.trace("Destroyed previous socket");
        }

        int socketType = ZMQ.DEALER;
        socket = ctx.createSocket(socketType);
        socket.setIdentity(address.getBytes(Charsets.UTF_8));
        socket.connect(endpoint);

        poller = new ZPoller(ctx.createSelector());
        poller.register(socket, ZPoller.IN);

        log.trace("Connection to broker at {}", endpoint);
    }

    public void send(T payload) {

        writeRequest(payload);
        if (writtenRequests % writeBurst == 0)
            gatherResponses();
    }

    private void gatherResponses() {

        log.debug("Starting response gathering");

        long timeout = 100;

        // Dont stop gathering responses until we received ALL!
        while (reorderBuffer.hasPendingRequests()) {

            if (poller.poll(timeout) == -1)
                break; // Interrupted

            if (poller.isReadable(socket)) {

                ZMsg response = ZMsg.recvMsg(socket);
                log.trace("[{}] Received message from broker {}", address, ZHelper.dump(response, log.isTraceEnabled()));

                response.pop(); // EMPTY SEPARATOR

                BatchRequest<T> batchResponse = BatchRequest.fromMsg(serializer, response);

                reorderBuffer.completed(batchResponse);
                Iterable<BatchRequest<T>> okReq = reorderBuffer.gatherCompletedInOrder();
                for (BatchRequest<T> breq : okReq) {
                    for(T data : breq)
                        asyncHandler.handle(data);
                }

            } else {

                Collection<BatchRequest<T>> pendingRequests = reorderBuffer.getPendingRequests();
                log.debug("Pending requests : {}", pendingRequests.size());
                for (BatchRequest timeoutReq : pendingRequests) {
                    if (timeoutReq.getRetries() < 1000) { // Cambiar a 0
                        timeoutReq.failRequest();
                        timeoutReq.toMsg(serializer, CMD.CLIENT, CMD.REQUEST, requestedService, null).send(socket);
                    } else
                        log.error("Batch Request {} failed {} times", timeoutReq.batchNo, retries);
                }
            }
        }

        log.debug("Finished response gathering");
    }

    private void writeRequest(T payload) {
        currentBatch.addReq(payload);

        if (currentBatch.isReady()) {
            ZMsg msg = currentBatch.toMsg(serializer, CMD.CLIENT, CMD.REQUEST, requestedService, null);
            log.trace("[{}] Sending message {}", address, ZHelper.dump(msg, log.isTraceEnabled()));
            msg.send(socket); // Send the batch message

            reorderBuffer.pending(currentBatch);
            writtenRequests += 1;
            currentBatch = new BatchRequest<>(currentBatch.batchNo + 1, batchSize);
        }
    }

    /**
     * Con los parametros por defecto, no se recuperaran respuestas hasta
     * que se hayan mandado 25 lotes * 2000 lotes/rafaga = 5000 registros
     * <p>
     * Si se construye un cliente con estos parÃ¡metros y sÃ³lo se 'envÃ­an'
     * 3000 registros, el cliente se quedarÃ¡ esperando los 2000 registros
     * faltantes.
     * <p>
     * Este mÃ©todo permite forzar el envÃ­o y posterior recepciÃ³n de esas X
     * peticiones cuando X es menor que la cantidad de mensajes esperados
     */
    public void flush() {
        log.info("Flush invoked");
        currentBatch.forceReady();
        writeRequest(null); // currentBatch.addReq discards null payloads
        gatherResponses();
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Override
    public void close() {
        flush();
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
        private int batchSize = 25; // req per message
        private int writeBurst = 5000; // messages until readFrom wait
        private int timeout = 2500; // msecs
        private int retries = 3;
        private BiTransformer<T, byte[]> serializer;
        private ResponseHandler<T> handler;

        public static <T> Builder<T> start() {
            return new Builder<>();
        }

        public Builder<T> connectTo(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder<T> serializeUsing(BiTransformer<T, byte[]> serializer) {
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

        public Builder<T> writeBurst(int writeBurst) {
            this.writeBurst = writeBurst;
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