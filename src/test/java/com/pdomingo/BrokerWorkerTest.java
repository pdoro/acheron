package com.pdomingo;

import com.pdomingo.broker.Broker;
import com.pdomingo.worker.Worker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * BrokerWorkerTest
 * @author pdomingo
 */
public class BrokerWorkerTest {

    public static void main(String[] args) throws InterruptedException {

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Thread.sleep(500);

        executor.submit(new Runnable() {
            @Override
            public void run() {
                Broker broker = new Broker("tcp://*:5555");
            }
        });

        executor.submit(new Runnable() {
            @Override
            public void run() {
                Worker worker = new Worker("tcp://localhost:5555", 1);
            }
        });

        executor.shutdown();
    }
}