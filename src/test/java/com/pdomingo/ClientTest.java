package com.pdomingo;

import com.pdomingo.client.Client;

/**
 * Created by Pablo on 1/4/17.
 */
public class ClientTest {

    public static void main(String[] args) {

        Client<String> client = Client.Builder.<String>
                start()
                .async(true)
                .connectTo("tcp://localhost:8989")
                .timeout(2500)
                .retries(3)
                .batchSize(15000)
                .responseHandler(new Client.ResponseHandler<String>() {
                    @Override
                    public void handle(String response) {

                    }
                })
                .build();


    }
}
