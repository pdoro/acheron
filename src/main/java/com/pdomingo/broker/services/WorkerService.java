package com.pdomingo.broker.services;

import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

import java.util.Queue;

import com.pdomingo.broker.Broker;
import com.pdomingo.broker.Broker.VWorker;
import com.pdomingo.zmq.CMD;
import com.pdomingo.zmq.ZHelper;
import lombok.extern.slf4j.Slf4j;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.*;

/**
 * WorkerService
 * @author pdomingo
 */
@Slf4j
public class WorkerService implements Service {

    private final String serviceName;
    private Queue<VWorker> availableWorkers;
    private Map<String, VWorker> workerSet;

    public WorkerService(String serviceName) {
        this.serviceName = serviceName;
        availableWorkers = new LinkedList<>();
        workerSet = new HashMap<>();
    }

    @Override
    public void handle(ZFrame requester, ZMsg payload, Broker brokerContext) {

        if(log.isTraceEnabled())
            log.trace("Received message from {}: {}", requester.toString(), ZHelper.dump(payload));

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

            if(availableWorkers.isEmpty()) {
                // No hay workers disponibles
                // En un futuro, mandar la peticiÃƒÂ³n a otro broker que tenga
                // workers disponibles para trabajar
                // Ã‚Â¿Y si el broker no dispone de ese servicio?
            } else {
                VWorker freeWorker = availableWorkers.poll();
                ZMsg msg = freeWorker.buildRequest(sender, payload);
                brokerContext.send(msg);
            }
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

                ZFrame client = msg.unwrap();               // ----------------------- //
                msg.add(new ZFrame(ZMQ.MESSAGE_SEPARATOR)); // | 0  - EMPTY_STRING   | //
                msg.add(client);                            // | 1  - Client address | //
                msg.add(CMD.CLIENT.newFrame());             // | 2  - CMD.CLIENT     | //
                //msg.add(worker.service);                  // | 3+ - Payload        | //
                // Revisar				                    // ----------------------- //
                brokerContext.send(msg);
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
