package com.pdomingo.broker;

import com.pdomingo.broker.services.Service;
import com.pdomingo.broker.services.WorkerService;
import com.pdomingo.zmq.CMD;
import com.pdomingo.zmq.Heartbeat;
import com.pdomingo.zmq.ZHelper;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.zeromq.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Pablo on 4/4/17.
 */
@Slf4j
public class Broker {

    private final ZContext ctx;
    private ZMQ.Socket socket;
    private final boolean verbose;

    private Map<String, Service> services;
    private Map<String, Worker> workers;

    private static final String INTERNAL_SERVICE_PREFIX = "mmi.";

    @Value
    public static class Worker {
        public String address;
        public Heartbeat heart = new Heartbeat();

        public ZMsg buildRequest(ZFrame sender, ZMsg payload) {
            ZMsg msg = new ZMsg();                      // ----------------------- //
            msg.add(new ZFrame(ZMQ.MESSAGE_SEPARATOR)); // | 0  - EMPTY_STRING   | //
            msg.add(CMD.REQUEST.newFrame());            // | 1  - CMD.REQUEST    | //
            msg.add(sender);                            // | 2  - Client address | //
            msg.addAll(payload);                        // | 3+ - Payload        | //
            return msg;                                 // ----------------------- //
        }
    }

    public Broker(String endpoint) {

        verbose = true;

        ctx = new ZContext();

        socket = ctx.createSocket(ZMQ.ROUTER);
        socket.setIdentity("Broker01".getBytes());
        socket.bind(endpoint);

        log.trace("Successful binding to endpoint {}", endpoint);

        services = new HashMap<>();
        workers  = new HashMap<>();

        log.info("Broker started");

        run();
    }

    private void run() {

        ZPoller poller = new ZPoller(ctx.createSelector());
        poller.register(socket, ZPoller.IN);

        while (!Thread.currentThread().isInterrupted()) {

            if(poller.poll(Heartbeat.HEARTBEAT_INTERVAL) == -1)
                break; // Interrupted

            if(poller.isReadable(socket)) {

                ZMsg msg = ZMsg.recvMsg(socket);
                if (msg == null)
                    break; // Interrupted

                if (log.isTraceEnabled())
                    log.trace("Received message: {}", ZHelper.dump(msg));

                ZFrame sender = msg.pop();
                ZFrame empty = msg.pop();
                ZFrame service = msg.pop();

                Service requestedService = services.get(service.toString());
                if(requestedService == null)
                    registerService(service, sender, msg);
                else
                    requestedService.handle(sender, msg, this);

            }

            purgeWorkers();
            sendHeartbeats();
        }

        ctx.destroy();
    }

    private void registerService(ZFrame service, ZFrame sender, ZMsg msg) {

        // Only workers are allowed to register a service
        // Any other request will be log and dropped

        // Avoid message destruction
        Iterator<ZFrame> iter = msg.iterator();

        ZFrame command = iter.hasNext() ? iter.next() : null;  // | 0 - CMD.WORKER     | //
        ZFrame ready   = iter.hasNext() ? iter.next() : null;  // | 1 - CMD.READY      | //

        // Message does not conform to WORKER READY structure.
        if(service == null || command	== null || ready == null) {
            log.warn("Requested service {} doesn't exist. Dropping message", service);
        } else {

            String serviceName = service.toString();

            boolean isWorker = CMD.resolveCommand(command) == CMD.WORKER;
            boolean isReady  = CMD.resolveCommand(ready)   == CMD.READY;

            if(isWorker && isReady) {

                Service workerService = new WorkerService(serviceName);
                services.put(serviceName, workerService);
                log.info("Registered service external '{}'", serviceName);

                workerService.handle(sender, msg, this);
            }
        }
    }

    private void sendHeartbeats() {

        log.info("Started broker heartbeat cycle");
        int notifiedWorkers = 0;

        for(Worker worker : workers.values())
            if(worker.heart.timeToBeat()) {
                ZMsg msg = worker.heart.sendHeartbeat(worker.address);
                log.trace("Sent heartbeat to worker {} {}", worker.address, msg);
                msg.send(socket); // falta la direccion de envio!
                notifiedWorkers++;
            }

        log.info("Finished broker heartbeat cycle. Total workers notified: {}", notifiedWorkers);
    }

    private void purgeWorkers() {

        log.info("Started inactive worker purge cycle");

        Iterator<Map.Entry<String,Worker>> iter = workers.entrySet().iterator();
        int failedWorkers = 0;
        int purgedWorkers = 0;

        while(iter.hasNext()) {

            Map.Entry<String, Worker> entry = iter.next();
            Heartbeat heart = entry.getValue().heart;

            if (heart.heartbeatExpired()){
                heart.fail();
                log.trace("Worker '{}' heartbeat expired", entry.getValue().address);
                failedWorkers++;

                if (heart.seemsDead()) {
                    log.trace("Endpoint declared dead, purging worker: '{}'", entry.getValue().address);
                    iter.remove();
                    purgedWorkers++;

					/* The broker will disconnect any irresponsive worker.
					 * It can't wait until the worker is available
					 * (as workers do) because it must serve other requests.
					 * If the worker was alive, it will start to fail broker
					 * beats so it will reconnect within a few seconds
					 */
                }
            }
        }

        log.info("Finished worker purge cycle. Failed workers: {}, Purged workers: {}", failedWorkers, purgedWorkers);
    }

    public void registerWorker(String address, Worker worker) {
        log.trace("Registered worker '{}'", address);
        workers.put(address, worker);
    }

    /**
     * Allows services to use the broker socket
     * wihtout providing it directly
     * @param msg
     */
    public void send(ZMsg msg) {
        msg.send(socket);
    }
}