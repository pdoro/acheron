package com.pdomingo;

import com.pdomingo.broker.Broker;
import com.pdomingo.worker.Worker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * BrokerWorkerTest
 * @author pdomingo
 */
public class BrokerWorkerTest {

    public static void main(String[] args) throws InterruptedException {

        ExecutorService executor = Executors.newCachedThreadPool();

        executor.submit(new Runnable() {
            @Override
            public void run() {
                new Broker("tcp://*:5555");
            }
        });

        Thread.sleep(300);

        for (int idx = 0; idx < 2; idx++) {
            final int id = idx;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    new Worker("tcp://localhost:5555", id);
                }
            });
            Thread.sleep(300);
        }

        executor.shutdown();
        executor.awaitTermination(1000000L, TimeUnit.DAYS);
    }
}