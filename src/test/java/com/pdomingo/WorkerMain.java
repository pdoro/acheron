package com.pdomingo;

import com.pdomingo.worker.Worker;

/**
 * Created by Pablo on 6/4/17.
 */
public class WorkerMain {

    public static void main(String[] args) {
        new Worker("tcp://localhost:5555", 1);
    }
}
