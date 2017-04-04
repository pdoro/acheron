package com.pdomingo.broker.services;

import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

import java.util.Queue;

/**
 * Created by Pablo on 4/4/17.
 */
public class WorkerService implements Service {

    private Queue<Worker> workerQueue;

    public static class Worker {
        private ZFrame address;
    }

    @Override
    public void handle(ZMsg payload) {

    }
}
