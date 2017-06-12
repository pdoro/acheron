package com.pdomingo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.pdomingo.broker.Broker;
import com.pdomingo.client.Client;
import com.pdomingo.pipeline.transform.Serializer;
import com.pdomingo.worker.Worker;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Created by pdomingo on 28/05/17.
 */
@Slf4j
public class AcheronTest {

    private static final String BROKER_ENDPOINT = "tcp://localhost:8765";
    private final ExecutorService threadPool = Executors.newCachedThreadPool();

    public static final String RESPONSE_HEADER = "OK-";
    public static final String REQUEST_HEADER = "Test-";

    @Test
    public void asyncTest() throws InterruptedException {

        threadPool.submit(new ClientRunnable(true));

        threadPool.submit(new WorkerRunnable());
        threadPool.submit(new WorkerRunnable());
        threadPool.submit(new WorkerRunnable());
        threadPool.submit(new WorkerRunnable());

        threadPool.submit(new BrokerRunnable());

        threadPool.shutdown();
        threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }


    @Test
    public void syncTest() throws InterruptedException {

        threadPool.submit(new ClientRunnable(false));

        threadPool.submit(new WorkerRunnable());
        threadPool.submit(new WorkerRunnable());
        threadPool.submit(new WorkerRunnable());
        threadPool.submit(new WorkerRunnable());

        threadPool.submit(new BrokerRunnable());

        threadPool.shutdown();
        threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    @AllArgsConstructor
    @ToString
    public static class DummyObj {
        int id;
        String text;
    }

    public static class DummyObjSerializer
            extends com.esotericsoftware.kryo.Serializer<DummyObj>
            implements Serializer<DummyObj> {

        private final Kryo kryo = new Kryo();
        private Output output = new Output(1024);

        @Override
        public void write(Kryo kryo, Output output, DummyObj dummyObj) {
            output.writeInt(dummyObj.id);
            output.writeString(dummyObj.text);
        }

        @Override
        public DummyObj read(Kryo kryo, Input input, Class<DummyObj> aClass) {

            int id = input.readInt();
            String text = input.readString();

            return new DummyObj(id, text);
        }

        @Override
        public byte[] write(DummyObj dummyObj) {
            Output output = new Output(1024);
            write(kryo, output, dummyObj);
            return output.toBytes();
        }

        @Override
        public DummyObj read(byte[] bytes) {
            return read(kryo, new Input(bytes), DummyObj.class);
        }
    }

    public static class ClientRunnable implements Runnable {

        private boolean async;

        public ClientRunnable(boolean async) {
            this.async = async;
        }

        @Override
        public void run() {
            Client<DummyObj> client = Client.Builder.<DummyObj>start()
                    .async(async)
                    .batchSize(1000)
                    .writeBurst(50)
                    .connectTo(BROKER_ENDPOINT)
                    .serializeUsing(new DummyObjSerializer())
                    .responseHandler(new Client.ResponseHandler<DummyObj>() {
                        @Override
                        public void handle(DummyObj response) {
                            assertEquals(response.text, RESPONSE_HEADER + response.id);
                            log.error("Response@Client: {}", response);
                        }
                    })
                    .retries(50)
                    .timeout(1000)
                    .forService("process")
                    .build();


                for (int idx = 0; idx < 5_000_000; idx++) {
                    client.send(new DummyObj(idx, REQUEST_HEADER + idx));
                }


            client.flush();
            client.close();
        }
    }

    public static class WorkerRunnable implements Runnable {

        @Override
        public void run() {
            Worker<DummyObj> worker = Worker.Builder.<DummyObj>start()
                .connectTo(BROKER_ENDPOINT)
                .forService("process")
                .serializeUsing(new DummyObjSerializer())
                .useFunction(new Function<DummyObj, DummyObj>() {
                    @Override
                    public DummyObj apply(DummyObj dummyObj) {
                        assertEquals(dummyObj.text, REQUEST_HEADER + dummyObj.id);
                        log.error("Request@Worker: {}", dummyObj);
                        dummyObj.text = RESPONSE_HEADER + dummyObj.id;
                        return dummyObj;
                    }
                }).build();

            worker.start();
        }
    }

    public static class BrokerRunnable implements Runnable {

        @Override
        public void run() {
            Broker broker = new Broker(BROKER_ENDPOINT);
            broker.start();
        }
    }
}
