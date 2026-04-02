package com.example.kvservice;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class KvServer {

    private final Server server;
    private final TarantoolClientWrapper tarantool;

    public KvServer(int grpcPort, String tarantoolHost, int tarantoolPort,
                    String tarantoolUser, String tarantoolPassword) throws Exception {

        this.tarantool = new TarantoolClientWrapper(
                tarantoolHost, tarantoolPort, tarantoolUser, tarantoolPassword
        );

        KvServiceImpl kvService = new KvServiceImpl(tarantool);

        this.server = ServerBuilder
                .forPort(grpcPort)
                .addService(kvService)
                .build();
    }

    public void start() throws Exception {
        server.start();
        System.out.println("gRPC server started on port: " + server.getPort());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            server.shutdown();
            try {
                tarantool.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

        server.awaitTermination();
    }

    public static void main(String[] args) throws Exception {
        int grpcPort = 8080;
        String tarantoolHost = "localhost";
        int tarantoolPort = 3301;
        String tarantoolUser = "kvuser";
        String tarantoolPassword = "kvpassword";

        KvServer kvServer = new KvServer(
                grpcPort, tarantoolHost, tarantoolPort,
                tarantoolUser, tarantoolPassword
        );
        kvServer.start();
    }
}