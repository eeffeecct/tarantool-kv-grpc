package com.example.kvservice;

import com.example.kvservice.grpc.*;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class KvServiceImpl extends KvServiceGrpc.KvServiceImplBase {

    private final TarantoolClientWrapper tarantool;

    public KvServiceImpl(TarantoolClientWrapper tarantool) {
        this.tarantool = tarantool;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        try {
            byte[] value = request.hasValue() ? request.getValue().toByteArray() : null;
            tarantool.put(request.getKey(), value);
            // empty ack
            responseObserver.onNext(PutResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription(e.getMessage())
                            .asRuntimeException()
            );
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            TarantoolClientWrapper.GetResult result = tarantool.get(request.getKey());
            if (!result.found) {
                responseObserver.onError(
                        Status.NOT_FOUND
                                .withDescription("Key not found: " + request.getKey())
                                .asRuntimeException()
                );
                return;
            }

            GetResponse.Builder builder = GetResponse.newBuilder();
            if (result.value != null) {
                builder.setValue(ByteString.copyFrom(result.value));
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription(e.getMessage())
                            .asRuntimeException()
            );
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        try {
            tarantool.delete(request.getKey());

            responseObserver.onNext(DeleteResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription(e.getMessage())
                            .asRuntimeException()
            );
        }
    }

    @Override
    public void range(RangeRequest request, StreamObserver<KeyValue> responseObserver) {
        try {
            tarantool.range(
                    request.getKeySince(),
                    request.getKeyTo(),
                    (key, value) -> {
                        KeyValue.Builder kvBuilder = KeyValue.newBuilder().setKey(key);
                        if (value != null) {
                            kvBuilder.setValue(ByteString.copyFrom(value));
                        }
                        responseObserver.onNext(kvBuilder.build());
                    }
            );
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription(e.getMessage())
                            .asRuntimeException()
            );
        }
    }

    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        try {
            long count = tarantool.count();
            responseObserver.onNext(
                    CountResponse.newBuilder()
                            .setCount(count)
                            .build()
            );
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription(e.getMessage())
                            .asRuntimeException()
            );
        }
    }
}