package kvstore.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import kvstore.common.WriteReq;
import kvstore.common.WriteResp;
import kvstore.servers.MasterServiceGrpc;

import java.util.logging.Logger;

public class KVClient {
    private static final Logger logger = Logger.getLogger(KVClient.class.getName());

    void write(String key, String value) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 12345)
                .usePlaintext()
                .build();
        MasterServiceGrpc.MasterServiceBlockingStub stub = MasterServiceGrpc.newBlockingStub(channel);
        WriteReq writeReq = WriteReq.newBuilder()
                .setKey(key)
                .setVal(value)
                .build();
        WriteResp resp = stub.writeMsg(writeReq);
        logger.info(String.format("RPC %d: Worker[%d] has done", resp.getStatus(), resp.getReceiver()));
        channel.shutdown();
    }

    public static void main(String[] args) {
        KVClient client = new KVClient();
        client.write("hello", "world");
        client.write("Bye", "bye");
    }
}
