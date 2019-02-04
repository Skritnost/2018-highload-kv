package ru.mail.polis.impl;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.KVDao;
import ru.mail.polis.impl.KVDaoImpl;
import ru.mail.polis.impl.RF;
import java.io.IOException;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.*;

public abstract class RequestHandler {
    private final String methodName;
    @NotNull
    final KVDaoImpl dao;
    @NotNull
    private final RF rf;
    final String id;
    final Long bytes;
    final byte[] value;

    final String TIMESTAMP = "timestamp";
    final String STATE = "state";

    private Logger log = LoggerFactory.getLogger(RequestHandler.class);

    RequestHandler(String methodName, @NotNull KVDao dao, @NotNull RF rf, String id, Long bytes, byte[] value) {
        this.methodName = methodName;
        this.dao = (KVDaoImpl) dao;
        this.rf = rf;
        this.id = id;
        this.bytes = bytes;
        this.value = value;
    }
    public abstract Response proxied() throws NoSuchElementException;
    public abstract Callable<Boolean> local() throws IOException;
    public abstract Callable<Boolean> httpClient(HttpClient client);
    abstract Response onSuccess(int acks);
    abstract Response onFail(int acks);
    public Response getResponse(ArrayList<Future<Boolean>> futures) {
        int acks = 0;

        for (Future<Boolean> future : futures) {
            try {
                if (future.get()) {
                    acks++;
                    if (acks >= rf.getAck()) {
                        return onSuccess(acks);
                    }
                }
            } catch (ExecutionException | InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }

        return onFail(acks);
    }

    Response gatewayTimeout(int acks) {
        log.info("Operation " + methodName + " not fulfield, acks = " + acks + " ; requiments - " + rf);
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    Response success(String responseName, int acks, byte[] body) {
        log.info("Operation " + methodName + " fulfield in  " + acks + " nods; requiments - " + rf);
        return new Response(responseName, body);
    }
}