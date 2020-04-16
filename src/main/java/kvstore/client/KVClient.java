package kvstore.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import kvstore.common.WriteReq;
import kvstore.common.WriteResp;
import kvstore.servers.MasterServiceGrpc;
import kvstore.servers.ClientServiceGrpc;
import kvstore.servers.ClientMsg;
import kvstore.servers.ServerResponse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class KVClient {
    private static final Logger logger = Logger.getLogger(KVClient.class.getName());
    private int clientId;
    private int port;
    private String ip;
    private int serverPort;
    private String serverIp;
    private List<ClientQuest> reqList;

    public KVClient (String configuration, int clientId) {
        this.clientId = clientId;
        reqList = new ArrayList<>();
        try{
          parse(configuration);
        } catch (IOException e){
          //
        }
    }

    private void parse(String configuration) throws IOException {
        List<String> allLines = Files.readAllLines(Paths.get(configuration));
        for (String line : allLines) {
            String[] conf = line.split(":");
            switch (conf[0]) {
                case "client":
                    this.ip = conf[1];
                    this.port = Integer.parseInt(conf[2]);
                    break;
                case "server":
                    this.serverIp = conf[1];
                    this.serverPort = Integer.parseInt(conf[2]);
                    break;
                case "request":
                    reqList.add(new ClientQuest(conf[1], conf[2], conf[3], conf[4]));
                    break;
                default:
                    System.err.println("Unknown conf: " + line);
            }
        }
    }

    static class ClientQuest {
        public int action;
        public String key; // ip can be hostname as well
        public String value;
        public int option;

        public ClientQuest (String action, String key, String value, String option){
            this.action = Integer.parseInt(action);
            this.key = key;
            this.value = value;
            this.option = Integer.parseInt(option);
        }
    }

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

    private void sendMsgtoServer(ClientQuest req) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(serverIp, serverPort)
                .usePlaintext()
                .build();
        ClientServiceGrpc.ClientServiceBlockingStub stub = ClientServiceGrpc.newBlockingStub(channel);
        ClientMsg request = ClientMsg.newBuilder()
                .setClientId(clientId)
                .setAction(req.action)
                .setKey(req.key)
                .setValue(req.value)
                .setOption(req.option)
                .build();
        ServerResponse response = stub.sendRequest(request);

        logger.info(String.format("RPC: %d: Client[%d] send request", response.getStatus(), clientId));
        channel.shutdown();
    }

    public static void main(String[] args) {
        KVClient client= new KVClient(args[0], Integer.parseInt(args[1]));
        //logger.info(String.format("configuration complete"));
        for (ClientQuest req : client.reqList) {
            client.sendMsgtoServer(req);
        }
    }

    /**
    public static void main(String[] args) {
        KVClient client = new KVClient();
        client.write("hello", "world");
        client.write("Bye", "bye");
    }**/
}
