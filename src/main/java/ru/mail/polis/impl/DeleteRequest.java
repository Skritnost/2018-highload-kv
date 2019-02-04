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

public class DeleteHandler extends RequestHandler {

    public DeleteHandler(@NotNull String methodName, @NotNull KVDao dao, @NotNull RF rf, String id, Long bytes) {
        super(methodName, dao, rf, id, bytes,null);
    }

    @Override
    public Callable<Boolean> httpClient(HttpClient client) {
        return () -> {
            final Response response = client.delete(KVDaoServiceImpl.ENTITY_PATH + "?id=" + id + "&bytes=" + bytes, KVDaoServiceImpl.PROXY_HEADER);
            return response.getStatus() == 202;
        };
    }

    @Override
    public Callable<Boolean> local() {
        return () -> {
            dao.remove(id.getBytes(), bytes);
            return true;
        };
    }

    @Override
    public Response proxied() {
        dao.remove(id.getBytes());
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    @Override
    public Response onFail(int acks) {
        return gatewayTimeout(acks);
    }

    @Override
    public Response onSuccess(int acks) {
        return success(Response.ACCEPTED, acks, Response.EMPTY);
    }

    //202 - Если ack из from реплик подтвердили операцию, иначе 504

    @Override
    public Response getResponse(ArrayList<Future<Boolean>> futures) {
        return super.getResponse(futures);
    }
}