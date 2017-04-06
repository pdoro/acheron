package com.pdomingo.broker.services;

import com.pdomingo.broker.Broker;
import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

/**
 * Created by Pablo on 4/4/17.
 */
public class DiscoveryService implements Service {
    @Override
    public void handle(ZFrame requester, ZMsg payload, Broker brokerContext) {

    }
}
