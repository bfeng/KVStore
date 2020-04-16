package kvstore.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import kvstore.common.WriteReq;
import kvstore.common.WriteResp;
import kvstore.servers.MasterServiceGrpc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class KVClient {
    private static final Logger logger = Logger.getLogger(KVClient.class.getName());
    private int clientId;
    private int serverPort;
    private String serverIp;
    private List<ReqContent> reqList;

    public KVClient (String configuration, int clientId) {
        this.clientId = clientId;
        reqList = new ArrayList<>();
        try{
          parse(configuration);
          logger.info("configure server: "+ serverIp + ":"+Integer.toString(serverPort));
        } catch (IOException e){
          //
        }
    }

    private void parse(String configuration) throws IOException {
        List<String> allLines = Files.readAllLines(Paths.get(configuration));
        for (String line : allLines) {
            String[] conf = line.split(":");
            switch (conf[0]) {
                case "server":
                    this.serverIp = conf[1];
                    this.serverPort = Integer.parseInt(conf[2]);
                    break;
                case "request":
                    reqList.add(new ReqContent(conf[1], conf[2], conf[3], conf[4]));
                    break;
                default:
                    System.err.println("Unknown conf: " + line);
            }
        }
    }

    static class ReqContent {
        public int action;
        public String key;
        public String value;
        public int option;

        public ReqContent (String action, String key, String value, String option){
            switch (action) {
                case "GET":
                    this.action = 0;
                    break;
                case "SET":
                    this.action = 1;
                    break;
                default:
                    System.err.println("Undefined operation: " + action);
            }
            this.key = key;
            this.value = value;

            switch (option) {
                case "Sequential":
                    this.option = 0;
                    break;
                case "Causal":
                    this.option = 1;
                    break;
                case "Eventual":
                    this.option = 2;
                    break;
                case "Linear":
                    this.option = 3;
                    break;
                default:
                    System.err.println("Undefined consistency option: " + option);
            }
        }

        public int getAct() {
            return this.action;
        }

        public String getKey() {
            return this.key;
        }

        public String getVal() {
            return this.value;
        }

        public int getOpt() {
            return this.option;
        }
    }

    void write(String key, String value) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(serverIp, serverPort)
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
        KVClient client= new KVClient(args[0], Integer.parseInt(args[1]));
        for (ReqContent req : client.reqList) {
            logger.info(Integer.toString(req.getAct())+":"+req.getKey()+":"+req.getVal()+":"+Integer.toString(req.getOpt()));
            if (req.getAct() == 1) {
                client.write(req.getKey(), req.getVal());
            }
        }
    }
}
