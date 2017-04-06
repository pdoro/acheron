package com.pdomingo.broker.services;

import com.pdomingo.broker.Broker;
import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

public interface Service {
    void handle(ZFrame requester, ZMsg payload, Broker brokerContext);
}
