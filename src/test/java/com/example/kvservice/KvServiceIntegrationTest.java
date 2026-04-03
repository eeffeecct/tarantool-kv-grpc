package com.example.kvservice;

import com.example.kvservice.grpc.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KvServiceIntegrationTest {

    @Container
    private static final GenericContainer<?> tarantool = new GenericContainer<>("tarantool/tarantool:3.2")
            .withExposedPorts(3301)
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("init.lua"),
                    "/opt/tarantool/init.lua"
            )
            .withCommand("tarantool", "/opt/tarantool/init.lua")
            .waitingFor(Wait.forLogMessage(".*ready to accept requests.*", 1));

    private static io.grpc.Server grpcServer;
    private static ManagedChannel channel;
    private static KvServiceGrpc.KvServiceBlockingStub blockingStub;
    private static TarantoolClientWrapper tarantoolClient;

    @BeforeAll
    static void setUp() throws Exception {
        String host = tarantool.getHost();
        int port = tarantool.getMappedPort(3301);

        tarantoolClient = new TarantoolClientWrapper(
                host, port, "kvuser", "kvpassword"
        );

        KvServiceImpl kvService = new KvServiceImpl(tarantoolClient);
        grpcServer = io.grpc.ServerBuilder
                .forPort(0)
                .addService(kvService)
                .build()
                .start();

        channel = ManagedChannelBuilder
                .forAddress("localhost", grpcServer.getPort())
                .usePlaintext()
                .build();

        blockingStub = KvServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (grpcServer != null) {
            grpcServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (tarantoolClient != null) {
            tarantoolClient.close();
        }
    }

    @Test
    @Order(1)
    @DisplayName("PUT - save new key")
    void testPut() {
        PutRequest request = PutRequest.newBuilder()
                .setKey("key1")
                .setValue(ByteString.copyFromUtf8("value1"))
                .build();
        PutResponse response = blockingStub.put(request);
        assertNotNull(response);
    }

    @Test
    @Order(2)
    @DisplayName("PUT - overwrite existing key")
    void testPutOverwrite() {
        blockingStub.put(PutRequest.newBuilder()
                .setKey("overwrite-key")
                .setValue(ByteString.copyFromUtf8("old-value"))
                .build());
        blockingStub.put(PutRequest.newBuilder()
                .setKey("overwrite-key")
                .setValue(ByteString.copyFromUtf8("new-value"))
                .build());

        GetResponse response = blockingStub.get(
                GetRequest.newBuilder().setKey("overwrite-key").build()
        );

        assertEquals("new-value", response.getValue().toStringUtf8());
    }

    @Test
    @Order(3)
    @DisplayName("PUT - save with null value")
    void testPutWithNullValue() {
        PutRequest request = PutRequest.newBuilder()
                .setKey("null-value-key")
                .build();

        PutResponse response = blockingStub.put(request);
        assertNotNull(response);
    }

    @Test
    @Order(4)
    @DisplayName("GET - get exising key")
    void testGet() {
        blockingStub.put(PutRequest.newBuilder()
                .setKey("get-key")
                .setValue(ByteString.copyFromUtf8("get-value"))
                .build());

        GetResponse response = blockingStub.get(
                GetRequest.newBuilder().setKey("get-key").build()
        );

        assertTrue(response.hasValue());
        assertEquals("get-value", response.getValue().toStringUtf8());
    }

    @Test
    @Order(5)
    @DisplayName("GET - get key with null value")
    void testGetWithNullValue() {
        blockingStub.put(PutRequest.newBuilder()
                .setKey("null-get-key")
                .build());

        GetResponse response = blockingStub.get(
                GetRequest.newBuilder().setKey("null-get-key").build()
        );
        assertFalse(response.hasValue());
    }

    @Test
    @Order(6)
    @DisplayName("GET - nonexisting key should return NOT_FOUND")
    void testGetNotFound() {
        StatusRuntimeException exception = assertThrows(
                StatusRuntimeException.class,
                () -> blockingStub.get(
                        GetRequest.newBuilder().setKey("nonexistent-key").build()
                )
        );
        // check error status
        assertEquals(io.grpc.Status.NOT_FOUND.getCode(), exception.getStatus().getCode());
    }

    @Test
    @Order(7)
    @DisplayName("DELETE - delete existing key")
    void testDelete() {
        blockingStub.put(PutRequest.newBuilder()
                .setKey("delete-key")
                .setValue(ByteString.copyFromUtf8("to-delete"))
                .build());

        blockingStub.delete(
                DeleteRequest.newBuilder().setKey("delete-key").build()
        );

        assertThrows(StatusRuntimeException.class, () ->
                blockingStub.get(GetRequest.newBuilder().setKey("delete-key").build())
        );
    }

    @Test
    @Order(8)
    @DisplayName("DELETE - delete unexsisting key (not an error)")
    void testDeleteNonExistent() {
        assertDoesNotThrow(() ->
                blockingStub.delete(
                        DeleteRequest.newBuilder().setKey("never-existed").build()
                )
        );
    }

    @Test
    @Order(9)
    @DisplayName("RANGE - range sampling")
    void testRange() {
        String[] keys = {"range-a", "range-b", "range-c", "range-d", "range-e"};
        for (int i = 0; i < keys.length; i++) {
            blockingStub.put(PutRequest.newBuilder()
                    .setKey(keys[i])
                    .setValue(ByteString.copyFromUtf8("val-" + i))
                    .build());
        }

        Iterator<KeyValue> stream = blockingStub.range(
                RangeRequest.newBuilder()
                        .setKeySince("range-b")
                        .setKeyTo("range-d")
                        .build()
        );

        List<String> resultKeys = new ArrayList<>();
        while (stream.hasNext()) {
            resultKeys.add(stream.next().getKey());
        }

        assertEquals(3, resultKeys.size());
        assertEquals("range-b", resultKeys.get(0));
        assertEquals("range-c", resultKeys.get(1));
        assertEquals("range-d", resultKeys.get(2));
    }

    @Test
    @Order(10)
    @DisplayName("RANGE - empty range")
    void testRangeEmpty() {
        Iterator<KeyValue> stream = blockingStub.range(
                RangeRequest.newBuilder()
                        .setKeySince("zzz-no-such-1")
                        .setKeyTo("zzz-no-such-2")
                        .build()
        );
        assertFalse(stream.hasNext());
    }

    @Test
    @Order(11)
    @DisplayName("COUNT - number of records")
    void testCount() {
        CountResponse response = blockingStub.count(
                CountRequest.newBuilder().build()
        );
        assertTrue(response.getCount() > 0);
    }

    @Test
    @Order(12)
    @DisplayName("5_000_000 records - put, get, range, count")
    @Timeout(value = 30, unit = TimeUnit.MINUTES)
    void testFiveMillionRecords() {

        System.out.println("Start 5_000_000 test... ");
        long startInsert = System.currentTimeMillis();

        int totalRecords = 5_000_000;
        int batchSize = 1_000;

        for (int i = 0; i < totalRecords; i += batchSize) {
            int from = i;
            int to = Math.min(i + batchSize, totalRecords);

            String lua =
                    "local from, to = ... " +
                    "box.begin() " +
                    "for i = from, to - 1 do " +
                    "    box.space.KV:replace({string.format('key-%08d', i)}) " +
                    "end " +
                    "box.commit() " +
                    "return to - from";

            tarantoolClient.evalRaw(lua, from, to);
            if ((i + batchSize) % 500_000 == 0) {
                System.out.println("Inserted: " + Math.min(i + batchSize, totalRecords) + " / " + totalRecords);
            }
        }

        long insertTime = System.currentTimeMillis() - startInsert;
        System.out.println("Inserted completed in " + (insertTime / 1000) + " seconds");

        System.out.println("Check count...");
        CountResponse countResponse = blockingStub.count(
                CountRequest.newBuilder().build()
        );
        assertTrue(countResponse.getCount() >= totalRecords,
                "Waiting for >= " + totalRecords + ", get " + countResponse.getCount());
        System.out.println("COUNT = " + countResponse.getCount() + " OK");

        System.out.println("Check get...");
        int[] testIndices = {0, 1, 999_999, 2_500_000, 4_999_999};
        for (int idx : testIndices) {
            String key = String.format("key-%08d", idx);

            GetResponse getResponse = blockingStub.get(
                    GetRequest.newBuilder().setKey(key).build()
            );

            assertFalse(getResponse.hasValue(),
                    "Key " + key + " must have null value");
        }
        System.out.println("GET works properly");

        System.out.println("Check put with value...");
        blockingStub.put(PutRequest.newBuilder()
                .setKey("key-00000000")
                .setValue(ByteString.copyFromUtf8("updated"))
                .build());

        GetResponse updatedResponse = blockingStub.get(
                GetRequest.newBuilder().setKey("key-00000000").build()
        );
        assertTrue(updatedResponse.hasValue());
        assertEquals("updated", updatedResponse.getValue().toStringUtf8());
        System.out.println("PUT with value works properly");

        System.out.println("Check range...");
        Iterator<KeyValue> rangeStream = blockingStub.range(
                RangeRequest.newBuilder()
                        .setKeySince("key-00001000")
                        .setKeyTo("key-00001009")
                        .build()
        );

        List<String> rangeKeys = new ArrayList<>();
        while (rangeStream.hasNext()) {
            rangeKeys.add(rangeStream.next().getKey());
        }

        assertEquals(10, rangeKeys.size(), "Range must return 10 records");
        assertEquals("key-00001000", rangeKeys.get(0));
        assertEquals("key-00001009", rangeKeys.get(9));
        System.out.println("RANGE works properly");

        System.out.println("Check delete...");
        blockingStub.delete(
                DeleteRequest.newBuilder().setKey("key-00000001").build()
        );

        assertThrows(StatusRuntimeException.class, () ->
                blockingStub.get(GetRequest.newBuilder().setKey("key-00000001").build())
        );
        System.out.println("DELETE works properly");

        System.out.println("=== Test with 5_000_000 records completed ===");
    }
}