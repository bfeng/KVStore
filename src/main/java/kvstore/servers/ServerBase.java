package kvstore.servers;

import io.grpc.Server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

abstract class ServerBase {
    private ServerConfiguration masterConf;
    private List<ServerConfiguration> workerConf;

    protected Server server;

    protected ServerBase(String configuration) throws IOException {
        workerConf = new ArrayList<>();
        // Pare the configuration when initiating the serverBase
        parse(configuration);
    }

    private void parse(String configuration) throws IOException {
        List<String> allLines = Files.readAllLines(Paths.get(configuration));
        for (String line : allLines) {
            String[] conf = line.split(":");
            switch (conf[0]) {
                case "master":
                    masterConf = new ServerConfiguration(conf[1], conf[2], conf[3], MachineType.master);
                    break;
                case "worker":
                    workerConf.add(new ServerConfiguration(conf[1], conf[2], conf[3], MachineType.worker));
                    break;
                default:
                    System.err.println("Unknown conf: " + line);
            }
        }
    }

    protected abstract void start() throws IOException;

    protected void stop() throws InterruptedException {
        if (server != null) {
            try {
                server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            } catch (NoClassDefFoundError e) {
                // ignore unknown bug belongs to grpc or netty
            }
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    protected void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public ServerConfiguration getMasterConf() {
        return this.masterConf;
    }

    public List<ServerConfiguration> getWorkerConf() {
        return this.workerConf;
    }

    enum MachineType {
        master,
        worker
    }

    /**
     * The serverConfiguration saves related metadata for the server
     * - A nested static inner class which is a static member of ServerBase class
     * - The final suggests the variable can only be assigned once
    */
    static class ServerConfiguration {
        final String ip; // ip can be hostname as well
        final int port;
        final String home;
        final MachineType type;

        public ServerConfiguration(String ip, String port, String home, MachineType type) {
            this.ip = ip;
            this.port = Integer.parseInt(port);
            this.home = home;
            this.type = type;
        }
    }

    enum ServerStatus {
        READY(0),
        DOWN(-1);

        private int value;

        ServerStatus(int value) {
            this.value = value;
        }

        int getValue() {
            return this.value;
        }
    }
}
