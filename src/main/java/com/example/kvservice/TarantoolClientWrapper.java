package com.example.kvservice;

import io.tarantool.client.TarantoolClient;
import io.tarantool.client.factory.TarantoolFactory;
import io.tarantool.pool.InstanceConnectionGroup;

import java.util.*;
import java.util.function.BiConsumer;

public class TarantoolClientWrapper {

    private static final int BATCH_SIZE = 1000;

    private final TarantoolClient client;

    public static class GetResult {
        public final boolean found;
        public final byte[] value;

        public GetResult(boolean found, byte[] value) {
            this.found = found;
            this.value = value;
        }
    }

    public TarantoolClientWrapper(String host, int port, String user, String password)
            throws Exception {

        InstanceConnectionGroup connectionGroup = InstanceConnectionGroup.builder()
                .withHost(host)
                .withPort(port)
                .withUser(user)
                .withPassword(password)
                .build();

        this.client = TarantoolFactory.box()
                .withGroups(Collections.singletonList(connectionGroup))
                .build();
    }

    public void put(String key, byte[] value) {
        client.eval(
                "return box.space.KV:replace({...})",
                Arrays.asList(key, value)
        ).join();
    }

    public GetResult get(String key) {
        List<?> result = client.eval(
                "local t = box.space.KV:get(...) " +
                "if t == nil then return nil end " +
                "return t[1], t[2]",
                Collections.singletonList(key),
                Object.class
        ).join().get();

        if (result.isEmpty() || result.get(0) == null) {
            return new GetResult(false, null);
        }

        byte[] value = null;
        if (result.size() > 1 && result.get(1) != null) {
            Object raw = result.get(1);
            if (raw instanceof byte[]) {
                value = (byte[]) raw;
            }
        }
        return new GetResult(true, value);
    }

    public void delete(String key) {
        client.eval(
                "return box.space.KV:delete(...)",
                Collections.singletonList(key)
        ).join();
    }

    public void range(String keySince, String keyTo,
                      BiConsumer<String, byte[]> consumer) throws Exception {

        String luaScript =
                "local key_from, key_to, batch_size, iter = ... " +
                "local result = {} " +
                "for _, t in box.space.KV.index.primary:pairs(key_from, " +
                "    {iterator = iter}) do " +
                "    if t[1] > key_to then break end " +
                "    table.insert(result, {t[1], t[2]}) " +
                "    if #result >= batch_size then break end " +
                "end " +
                "return unpack(result)";

        String currentKey = keySince;
        boolean isFirstBatch = true;

        while (true) {
            String iterator = isFirstBatch ? "GE" : "GT";
            isFirstBatch = false;

            List<?> batchResult = client.eval(
                    luaScript,
                    java.util.Arrays.asList(currentKey, keyTo, BATCH_SIZE, iterator),
                    Object.class
            ).join().get();

            if (batchResult == null || batchResult.isEmpty()) {
                break;
            }

            int count = 0;
            for (Object item : batchResult) {
                if (!(item instanceof List)) continue;
                List<?> pair = (List<?>) item;
                if (pair.isEmpty()) continue;

                Object keyObj = pair.get(0);
                if (keyObj == null) continue;

                String key = keyObj.toString();

                if (key.compareTo(keyTo) > 0) {
                    return;
                }

                byte[] value = null;
                if (pair.size() > 1 && pair.get(1) != null) {
                    Object raw = pair.get(1);
                    if (raw instanceof byte[]) {
                        value = (byte[]) raw;
                    }
                }

                consumer.accept(key, value);
                currentKey = key;
                count++;
            }

            if (count == 0 || count < BATCH_SIZE) {
                break;
            }
        }
    }

    public long count() {
        List<?> result = client.eval(
                "return box.space.KV:count()",
                Collections.emptyList(),
                Object.class
        ).join().get();

        return ((Number) result.get(0)).longValue();
    }

    public void close() throws Exception {
        client.close();
    }

    // for tests
    public void evalRaw(String lua, Object... args) {
        client.eval(lua, java.util.Arrays.asList(args)).join();
    }
}