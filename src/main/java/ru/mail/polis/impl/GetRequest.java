package ru.mail.polis.impl;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.impl.KVDaoServiceImpl;
import ru.mail.polis.impl.RF;
import ru.mail.polis.impl.Value;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class GetHandler extends RequestHandler {
    private List<Value> values = new ArrayList<>();
    private boolean partial;
    private long allSize;

    public GetHandler(@NotNull String methodName, @NotNull KVDao dao, @NotNull RF rf, String id, Long bytes, boolean partial, long allSize) {
        super(methodName, dao, rf, id, bytes, null);
        this.partial = partial;
        this.allSize = allSize;
    }

    @Override
    public Response proxied() throws NoSuchElementException {
        Value value = dao.internalGet(id.getBytes(), bytes);
        Response response = new Response(Response.OK, value.getData());
        response.addHeader(TIMESTAMP + value.getTimestamp());
        response.addHeader(STATE + value.getState().name());
        return response;
    }

    @Override
    public Callable<Boolean> httpClient(HttpClient client) {
        return () -> {
            final Response response = client.get(KVDaoServiceImpl.ENTITY_PATH + "?id=" + id + "&bytes=" + bytes, KVDaoServiceImpl.PROXY_HEADER);
            values.add(new Value(response.getBody(), Long.valueOf(response.getHeader(TIMESTAMP)),
                    Value.State.valueOf(response.getHeader(STATE))));
            return true;
        };
    }

    @Override
    public Callable<Boolean> local() {
        return () -> {
            Value value = dao.internalGet(id.getBytes(), bytes);
            values.add(value);
            return true;
        };
    }

    @Override
    public Response onFail(int acks) {
        return gatewayTimeout(acks);
    }

    @Override
    public Response onSuccess(int acks) throws NoSuchElementException {
        Value max = values
                .stream()
                .max(Comparator.comparing(Value::getTimestamp))
                .get();
        if (max.getState() == Value.State.PRESENT) {
            if (partial) {
                Response response = success(Response.PARTIAL_CONTENT, acks, max.getData());
                response.addHeader("Content-Type: multipart/byteranges");
                response.addHeader("Accept-Ranges: bytes");
                response.addHeader("Content-Range: bytes " + bytes + "-" + (bytes + max.getData().length - 1) + "/" + allSize);
                return response;
            } else {
                return success(Response.OK/*Response.PARTIAL_CONTENT*/, acks, max.getData());
            }
        } else {
            throw new NoSuchElementException();
        }
    }

    // тут 404 - если ни одна из ack реплик, что вернула ответ, не содержит данные (или же данные были удалены на одной из ack)

    @Override
    public Response getResponse(ArrayList<Future<Boolean>> futures) {
        return super.getResponse(futures);
    }
}