package com.pdomingo.client;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.primitives.Longs;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * Buffer de reordenacion de mensajes utilizado en {@link Client} para poder entregar
 * respuestas de manera secuencial y ordenada siguiendo un numero de secuencia
 * preestablecido
 * @param <T> tipo de dato
 */
@Slf4j
public class ReorderBuffer<T> {

    /* Expected number from pending 'task' to be able to flush the buffer */
    private long sequenceNumber;
    /* Requests awaiting response */
    private Map<String, BatchRequest<T>> pendingRequests;
    /* Buffer (priotity queue) that stores completed tasks but may include holes*/
    private MinMaxPriorityQueue<BatchRequest<T>> responses;

    /**
     *
     * @param bufferSize expected size of the buffer
     */
    public ReorderBuffer(int bufferSize) {

        this.sequenceNumber = 1;
        this.pendingRequests = new LinkedHashMap<>();
        this.responses = MinMaxPriorityQueue.orderedBy(
                new Comparator<BatchRequest>() {
                    @Override
                    public int compare(BatchRequest br1, BatchRequest br2) {
                        return Longs.compare(br1.batchNo, br2.batchNo);
                    }
                })
                .expectedSize(bufferSize)
                .create();
    }

    public Iterable<BatchRequest<T>> gatherCompletedInOrder() {

        boolean canFlush = ! responses.isEmpty() && sequenceNumber == responses.peekFirst().batchNo;
        List<BatchRequest<T>> orderedResponses = canFlush ? new LinkedList<>() : Collections.emptyList();

        while(canFlush) {
            BatchRequest<T> req = responses.pollFirst();
            orderedResponses.add(req);
            sequenceNumber += 1;
            canFlush = ! responses.isEmpty() && sequenceNumber == responses.peekFirst().batchNo;
        }

        return orderedResponses;
    }

    public void completed(BatchRequest<T> batchResponse) {
        if (pendingRequests.containsKey(batchResponse.batchId)) {
            // Not waiting this response anymore
            pendingRequests.remove(batchResponse.batchId);
            responses.offer(batchResponse);
        }
    }

    public void pending(BatchRequest<T> pendingRequest) {
        pendingRequests.put(pendingRequest.batchId, pendingRequest);
    }

    public boolean hasPendingRequests() {
        return !pendingRequests.isEmpty();
    }

    public Collection<BatchRequest<T>> getPendingRequests() {
        return pendingRequests.values();
    }
}
