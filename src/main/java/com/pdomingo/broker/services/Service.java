package com.pdomingo.broker.services;

import com.pdomingo.broker.Broker;
import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

public interface Service {

    /**
     * Handles the payload to the implementor service
     * @param requester sender of the original payload
     * @param payload og the message
     * @param brokerContext
     */
    void handle(ZFrame requester, ZMsg payload, Broker brokerContext);
}
