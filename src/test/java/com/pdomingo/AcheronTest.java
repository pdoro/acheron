package com.pdomingo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.pdomingo.broker.Broker;
import com.pdomingo.client.Client;
import com.pdomingo.pipeline.transform.BiTransformer;
import com.pdomingo.pipeline.transform.Transformer;
import com.pdomingo.worker.Worker;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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

    @AllArgsConstructor
    @ToString
    public static class DummyObj {
        int id;
        String text;
        Map<String, Status> statusMap;

        enum Status {
            OK, ERROR;
        }
    }

    public static class DummyObjSerializer
            extends Serializer<DummyObj>
            implements BiTransformer<DummyObj, byte[]> {

        private final Kryo kryo = new Kryo();
        private Output output = new Output(1024);
        private MapSerializer mapSerializer;

        public DummyObjSerializer() {
            mapSerializer = new MapSerializer();
            mapSerializer.setKeyClass(String.class, kryo.getSerializer(String.class));
            mapSerializer.setKeysCanBeNull(true);
            mapSerializer.setValueClass(DummyObj.Status.class, kryo.getSerializer(DummyObj.Status.class));
        }

        @Override
        public byte[] forwardTransform(DummyObj dummyObj) {
            write(kryo, output, dummyObj);
            byte[] byteArray = output.toBytes();
            output.clear();
            return byteArray;
        }

        @Override
        public DummyObj backwardTransform(byte[] bytes) {
            return read(kryo, new Input(bytes), DummyObj.class);
        }

        @Override
        public void write(Kryo kryo, Output output, DummyObj dummyObj) {
            output.writeInt(dummyObj.id);
            output.writeString(dummyObj.text);
            kryo.writeObject(output, dummyObj.statusMap, mapSerializer);
        }

        @Override
        public DummyObj read(Kryo kryo, Input input, Class<DummyObj> aClass) {

            int id = input.readInt();
            String text = input.readString();
            Map<String, DummyObj.Status> statusMap = kryo.readObject(input, HashMap.class, mapSerializer);

            return new DummyObj(id, text, statusMap);
        }
    }

    private static ExecutorService threadPool = Executors.newCachedThreadPool();


    //@Test
    //public void fullTest() throws InterruptedException {
    public static void main(String[] args) throws InterruptedException {

        threadPool.submit(new ClientRunnable());

        threadPool.submit(new WorkerRunnable());
        threadPool.submit(new WorkerRunnable());
        threadPool.submit(new WorkerRunnable());
        threadPool.submit(new WorkerRunnable());

        threadPool.submit(new BrokerRunnable());

        threadPool.shutdown();
        threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

    }

    public static class ClientRunnable implements Runnable {

        @Override
        public void run() {
            try {
            Client<DummyObj> client = Client.Builder.<DummyObj>start()
                    .async(true)
                    .batchSize(1000)
                    .writeBurst(50)
                    .connectTo(BROKER_ENDPOINT)
                    .serializeUsing(new DummyObjSerializer())
                    .responseHandler(new Client.ResponseHandler<DummyObj>() {
                        @Override
                        public void handle(DummyObj response) {
                            log.error("Response@Client: {}", response);
                        }
                    })
                    .retries(5)
                    .timeout(1000)
                    .build();


                for (int idx = 0; idx < 5_000_000; idx++) {
                    client.send(new DummyObj(idx, "Test_" + idx, Collections.emptyMap()));
                }


            client.flush();
            client.close();

            } catch (RuntimeException re) {
                log.error("Fatal error",re);
            }
        }
    }

    public static class WorkerRunnable implements Runnable {

        @Override
        public void run() {
            Worker<DummyObj> worker = new Worker<>(BROKER_ENDPOINT, new DummyObjSerializer(), new Function<DummyObj, DummyObj>() {
                @Override
                public DummyObj apply(DummyObj dummyObj) {
                    log.error("Request@Worker: {}", dummyObj);
                    return dummyObj;
                }
            });
            try {
            worker.start();
            } catch (RuntimeException re) {
                log.error("Fatal error",re);
            }
        }
    }

    public static class BrokerRunnable implements Runnable {

        @Override
        public void run() {
            Broker broker = new Broker(BROKER_ENDPOINT);
            try {
                broker.start();
            } catch (RuntimeException re) {
                log.error("Fatal error",re);
            }
        }
    }
}
