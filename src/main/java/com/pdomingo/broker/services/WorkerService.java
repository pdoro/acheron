package com.pdomingo.broker.services;

import com.pdomingo.broker.Broker;
import com.pdomingo.broker.Broker.VWorker;
import com.pdomingo.zmq.CMD;
import com.pdomingo.zmq.MsgBuilder;
import com.pdomingo.zmq.ZHelper;
import lombok.extern.slf4j.Slf4j;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.*;

/**
 *
 */
@Slf4j
public class WorkerService implements Service {

    private final String serviceName;

    private Queue<VWorker> availableWorkers;
    private Map<String, VWorker> busyWorkers;
    private Map<String, VWorker> workerSet;

    private TaskDistributionStrategy distributionStrategy;

    public WorkerService(String serviceName) {
        this.serviceName = serviceName;
        this.availableWorkers = new LinkedList<>();
        this.busyWorkers = new HashMap<>();
        this.workerSet = new HashMap<>();
    }

    @Override
    public void handle(ZFrame requester, ZMsg payload, Broker brokerContext) {

        log.trace("Received message from {}: {}", requester.toString(), ZHelper.dump(payload, log.isTraceEnabled()));

        ZFrame header = payload.pop();

        CMD command = CMD.resolveCommand(header);
        switch(command) {
            case CLIENT: processClient(requester, payload, brokerContext); break;
            case WORKER: processWorker(requester, payload, brokerContext); break;
            default:
                log.warn("Invalid header {}. Discarding message", command.name());
        }
    }

	/*--------------------------- Process Client Message ---------------------------*/

    private void processClient(ZFrame sender, ZMsg payload, Broker brokerContext) {

        CMD command = CMD.resolveCommand(payload.pop());

        if(command == CMD.REQUEST) {
            log.trace("Received request from {}", sender.toString());

			/*
			if(availableWorkers.isEmpty()) {

				sender.destroy();
				payload.destroy();
				log.warn("No available workers");

				// No hay workers disponibles
				// En un futuro, mandar la petición a otro broker que tenga
				// workers disponibles para trabajar
				// ¿Y si el broker no dispone de ese servicio?
			} else {
			*/

            VWorker freeWorker = availableWorkers.poll();
            if(freeWorker.alive) { // TODO solucionar posible NPException
                log.debug("Worker '{}' selected to interceptWrite request", freeWorker.address);
                ZMsg msg = freeWorker.buildRequest(sender, payload);

                log.trace("[{}] Sending message to worker {}", brokerContext.address, ZHelper.dump(msg, log.isTraceEnabled()));
                brokerContext.send(msg);
                availableWorkers.offer(freeWorker);
            }
            else {
                ;// TODO: manejar un worker que ha sido invalidado nivel de broker por HB fallido
            }
            //busyWorkers.put(freeWorker.address, freeWorker);

        }
        else
            log.warn("Invalid command {}. Discarding message", command.name());

        sender.destroy(); // TODO: REVISA!
        payload.destroy();
    }


	/*--------------------------- Process Worker Message ---------------------------*/

    /**
     *
     * @param sender
     * @param msg
     */
    private void processWorker(ZFrame sender, ZMsg msg, Broker brokerContext) {

        CMD command = CMD.resolveCommand(msg.pop());
        String address = sender.toString();
        VWorker worker = workerSet.get(address);

        log.trace("{} - from {}", command, address);

        switch(command) {

            case READY:
                // Add Worker to list of available workers
                worker = new VWorker(address);
                availableWorkers.add(worker);
                workerSet.put(address, worker);
                brokerContext.registerWorker(address, worker);
                // Idea aÃ±adir un payload con la latencia del mensaje
                break;

            case HEARTBEAT:
                break;

            case REPLY:

                ZMsg reply = MsgBuilder.start()      // -----------------------
                        .add(msg.pop())              // | 1  - Client address |
                        .add(ZMQ.MESSAGE_SEPARATOR)  // | 0  - EMPTY_STRING   |
                        .add(msg)                    // | 3+ - Payload        |
                        .build();                    // -----------------------

                log.trace("[{}] Sending reply to client {}", brokerContext.address, ZHelper.dump(reply, log.isTraceEnabled()));

                brokerContext.send(reply);

                //VWorker freeWorker = busyWorkers.remove(address);
                //availableWorkers.add(freeWorker);
                break;

            case DISCONNECT:
                availableWorkers.remove(worker); // Seria util un QueueSet (idea)
                workerSet.remove(address);
                log.trace("Removed worker {}", address);
                break;

            default:
                log.error("Invalid command : {}" + command);
        }

        worker.heart.beatFromEndpoint();
    }
}