package com.pdomingo;

import com.pdomingo.broker.Broker;

/**
 * Created by Pablo on 6/4/17.
 */
public class BrokerMain {

    public static void main(String[] args) {
        new Broker("tcp://*:5555");
    }
}
