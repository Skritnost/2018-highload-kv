package ru.mail.polis.impl;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.impl.KVDaoServiceImpl;
import ru.mail.polis.impl.RF;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class PutHandler extends RequestHandler {
    public PutHandler(@NotNull String methodName, @NotNull KVDao dao, @NotNull RF rf, String id, Long bytes, byte[] value) {
        super(methodName, dao, rf, id, bytes, value);
    }

    @Override
    public Response proxied() {
        dao.upsert(id.getBytes(), bytes, value);
        return new Response(Response.CREATED, Response.EMPTY);
    }

    @Override
    public Callable<Boolean> httpClient(HttpClient client) {
        return () -> {
            final Response response = client.put(KVDaoServiceImpl.ENTITY_PATH + "?id=" + id + "&bytes=" + bytes, value, KVDaoServiceImpl.PROXY_HEADER);
            return response.getStatus() == 201;
        };
    }

    @Override
    public Callable<Boolean> local() {
        return () -> {
            dao.upsert(id.getBytes(), bytes, value);
            return true;
        };
    }

    @Override
    public Response onSuccess(int acks) {
        return success(Response.CREATED, acks, Response.EMPTY);
    }

    @Override
    public Response onFail(int acks) {
        return gatewayTimeout(acks);
    }

    @Override
    public Response getResponse(ArrayList<Future<Boolean>> futures) {
        return super.getResponse(futures);
    }
}