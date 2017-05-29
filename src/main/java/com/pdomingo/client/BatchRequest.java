package com.pdomingo.client;

import com.pdomingo.pipeline.transform.BiTransformer;
import com.pdomingo.pipeline.transform.Transformer;
import com.pdomingo.zmq.CMD;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.primitives.Longs;
import com.pdomingo.zmq.MsgBuilder;
import lombok.Builder;
import lombok.Data;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * Agrupacion de elementos de tipo {@link T} que permite ser mandado
 * como un 'batch' desde un objeto {@link Client}. Esto permite reducir
 * en gran medida la latencia de los mensajes enviados y aumentar el
 * rendimiento envitando round-trips por mensajes unitarios.
 *
 * Esta clase permite transformarse en un {@link ZMsg} a traves del metodo
 * {@link #toMsg(Kryo, CMD, CMD, String, ZFrame)} y volver a transformarse
 * en destino mediante {@link #fromMsg(Kryo, ZMsg)}.
 *
 * Para poder de/serializar los objetos de tipo {@link T}, debe proporcionarse
 * un serializador de kryo a los metodos que permiten transformarse en mensaje
 */
@Data
public class BatchRequest<T> implements Iterable<T> {

    public final long batchNo;
    public String batchId;
    public final int batchSize;

    private List<T> requests;

    private int retries;
    private boolean forceReady;

    @Builder
    public BatchRequest(long batchNo, int batchSize) {
        this.batchSize = batchSize;
        this.batchNo = batchNo;
        this.batchId = UUID.randomUUID().toString();
        this.requests = new ArrayList<>(batchSize);

        this.retries = 0;
        this.forceReady = false;
    }

    @Builder
    public BatchRequest(String batchId, long batchNo,int batchSize) {
        this.batchSize = batchSize;
        this.batchNo = batchNo;
        this.batchId = batchId;
        this.requests = new ArrayList<>(batchSize);
        this.retries = 0;
        this.forceReady = false;
    }

    public boolean isReady() {
        return requests.size() == batchSize || forceReady;
    }

    public void forceReady() {
        this.forceReady = true;
    }

    public void addReq(T req) {
        if(req != null)
            requests.add(req);
    }

    public ZMsg toMsg(BiTransformer<T, byte[]> serializer, CMD requester, CMD msgType, String requestedService, ZFrame clientAddress) {

        MsgBuilder msgBuilder = MsgBuilder.start()
                .add(ZMQ.MESSAGE_SEPARATOR) // Frame 0 - empty (REQ compatibility) //
                .add(requestedService)      // Frame 1 - Service request           //
                .add(requester)              // Frame 2 - MDP Command               //
                .add(msgType);

        if(msgType == CMD.REPLY)
            msgBuilder.add(clientAddress);

        msgBuilder
                .add(batchId)
                .add(Longs.toByteArray(batchNo));

        for(T req : requests) {
            msgBuilder.add(serializer.forwardTransform(req));                     // Frame 3 - Request payload //
        }

        return msgBuilder.build();
    }

    @SuppressWarnings("unchecked")
    public static <T> BatchRequest<T> fromMsg(BiTransformer<T, byte[]> deserializer, ZMsg msg) {

        String batchId = msg.popString();
        long batchNo = Longs.fromByteArray(msg.pop().getData());
        BatchRequest<T> batchRequest = new BatchRequest<>(batchId, batchNo, msg.size());

        for (ZFrame frame : msg) {
            T deserializedFrame = deserializer.backwardTransform(frame.getData());
            batchRequest.addReq(deserializedFrame);
        }

        return batchRequest;
    }

    public void failRequest() {
        retries += 1;
    }

    @Override
    public Iterator<T> iterator() {
        return requests.iterator();
    }
}