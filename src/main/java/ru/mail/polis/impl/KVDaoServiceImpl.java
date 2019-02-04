package ru.mail.polis.impl;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

// Web-Server one nio

public class KVDaoServiceImpl extends HttpServer implements KVService {

    private final int port;
    @NotNull
    private final KVDaoImpl dao;
    @NotNull
    private final String[] topology;
    private final String me;
    private final RF defaultRF;
    private final Map<String, HttpClient> clients;
    private static final Logger log = LoggerFactory.getLogger(KVDaoServiceImpl.class);
    private final int THREAD_COUNT = 500;
    private final ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    public final static String PROXY_HEADER = "proxied";
    private final String STATUS_PATH = "/v0/status";
    public final static String ENTITY_PATH = "/v0/entity";

    public KVDaoServiceImpl(final int port,
                            @NotNull final KVDao dao,
                            @NotNull final Set<String> topology) throws IOException {
        super(create(port));
        this.port = port;
        this.dao = (KVDaoImpl) dao;
        this.topology = topology.toArray(new String[0]);
        defaultRF = new RF(this.topology.length / 2 + 1, this.topology.length);
        clients = topology
                .stream()
                .filter(node -> !node.endsWith(String.valueOf(port)))
                .collect(Collectors.toMap(
                        o -> o,
                        o -> new HttpClient(new ConnectionString(o))));

        me = Arrays
                .stream(this.topology)
                .filter(node -> node.endsWith(String.valueOf(port)))
                .findFirst()
                .orElseThrow(() -> {
                    IOException e = new IOException("Server " + port + " doesn't exist in topology of Clasters " + Arrays.toString(this.topology));
                    log.info(e.getMessage(), e);
                    return e;
                });

        log.info("Server " + port + " is started");
    }

    private static HttpServerConfig create(int port) {
        AcceptorConfig ac = new AcceptorConfig();
        ac.port = port;

        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ac};
        return config;
    }

    @Path(STATUS_PATH)
    public Response status() {
        return Response.ok("OK");
    }

    @Path(ENTITY_PATH)
    public void handler(Request request,
                        HttpSession session,
                        @Param("id=") String id,
                        @Param("bytes=") String bytesStr,
                        @Param("size=") String sizeStr,
                        @Param("replicas=") String replicas) throws IOException {
        log.info("Параметры запроса:\n" + request);

        if (id == null || id.isEmpty()) {
            log.error("id = " + id + " failed requirements");
            session.sendError(Response.BAD_REQUEST, null);
            return;
        }

        long bytes;
        boolean partial = false;
        if (bytesStr == null || bytesStr.isEmpty()) {
            String range = request.getHeader("Range:");
            if (range != null) {
                partial = true;
                bytes = Integer.parseInt(range.replace("bytes=", "").replace("-", "").trim());
            } else {
                bytes = 0;
            }
        } else {
            bytes = Long.parseLong(bytesStr);
            if (bytes < -1) {
                log.error("bytes = " + bytesStr + " failed requirements");
                session.sendError(Response.BAD_REQUEST, "Number of part must be === 0 or -1");
                return;
            }
        }

        long size = 0;
        if (sizeStr != null && !sizeStr.isEmpty()) {
            size = Long.parseLong(sizeStr);
            if (size <= 0) {
                log.error("size = " + sizeStr + " failed requirements");
                session.sendError(Response.BAD_REQUEST, "File size must be > 0");
                return;
            }
        }

        RF rf;
        if (replicas == null || replicas.isEmpty()) {
            rf = defaultRF;
        } else {
            try {
                rf = new RF(replicas);
            } catch (IllegalArgumentException e) {
                log.error(e.getMessage(), e);
                session.sendError(Response.BAD_REQUEST, null);
                return;
            }
        }

        String[] nodes;
        try {
            nodes = replicas(id, rf.getFrom());
        } catch (IllegalArgumentException e) {
            log.error(e.getMessage(), e);
            session.sendError(Response.BAD_REQUEST, null);
            return;
        }

        boolean proxied = request.getHeader(PROXY_HEADER) != null;
        log.info("Тип запроса - " + getMethodName(request.getMethod()) + "; proxied - " + proxied);
        try {

            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    session.sendResponse(customHandler(new GetRequest("GET", dao, rf, id, bytes, partial, size), nodes, proxied));
                    return;
                case Request.METHOD_PUT:
                    session.sendResponse(customHandler(new PutRequest("PUT", dao, rf, id, bytes, request.getBody()), nodes, proxied));
                    return;
                case Request.METHOD_DELETE:
                    byte[] metadata = dao.internalGet(id.getBytes(), 0).getData();
                    session.sendResponse(customHandler(new DeleteRequest("DELETE", dao, rf, id, 0L), nodes, proxied));

                    JSONObject obj = new JSONObject(new String(metadata));
                    long partQuantity = obj.getLong("chunkQuantity");
                    for (long i = 0; i < partQuantity; i++) {
                        customHandler(new DeleteRequest("DELETE", dao, rf, id, i), nodes, proxied);
                    }

                    return;
                default:
                    log.error(request.getMethod() + " неподдерживаемый код метод");
                    session.sendResponse(new Response(Response.METHOD_NOT_ALLOWED));
            }
        } catch (NoSuchElementException e) {
            log.info("Element " + id + " not found", e);
            session.sendError(Response.NOT_FOUND, null);
        } catch (IOException e) {
            log.error("Internal server error", e);
            log.error("Request parameters:\n" + request);
            session.sendError(Response.INTERNAL_ERROR, null);
        } catch (JSONException e) {
            log.info("Object with id=" + id + " is stored like 1 part");
            log.error(e.getMessage(), e);
        }
    }

    @Path("/v0/streaming")
    public void streaming(Request request,
                          HttpSession session,
                          @Param("id=") String id,
                          @Param("bytes=") String bytes,
                          @Param("size=") String size,
                          @Param("replicas=") String replicas) throws IOException, JSONException {
        log.info("Request parameters on streaming:\n" + request);

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                //возвращаем страницу
                if (id == null || id.isEmpty()) {
                    URL fileUrl = getClass().getClassLoader().getResource("streaming.html");
                    if (fileUrl != null) {
                        byte[] fileContent = Files.readAllBytes(new File(fileUrl.getPath()).toPath());
                        Response response = Response.ok(fileContent);
                        response.addHeader("Content-Type: text/html");

                        session.sendResponse(response);
                    } else {
                        session.sendError(Response.NOT_FOUND, null);
                    }
                } else {
                    handler(request, session, id, bytes, size, replicas);
                }
                break;
            case Request.METHOD_PUT:
                log.debug("PUT");
                if (bytes.equals("-1")) {
                    //сохранению данных
                    JSONObject json = new JSONObject(new String(request.getBody()));
                    if (!json.has("fileName") || !json.has("chunkQuantity")) {
                        session.sendError(Response.BAD_REQUEST, "Json must contained fields: fileName и chunkQuantity");
                        return;
                    }

                    String fileName = json.getString("fileName");
                    if (!json.has("type")) {
                        if (fileName.endsWith("webm")) {
                            json.put("type", "video");
                        } else if (fileName.endsWith("mp3")) {
                            json.put("type", "audio");
                        } else {
                            json.put("type", "text");
                        }
                        request.setBody(json.toString().getBytes());
                    }
                    handler(request, session, json.getString("fileName"), bytes, size, replicas);
                } else {
                    handler(request, session, id, bytes, size, replicas);
                }

                break;
            default:
                log.error(request.getMethod() + " unsupported code method");
                session.sendResponse(new Response(Response.METHOD_NOT_ALLOWED));
        }
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        log.error("Unsupported request \n" + request);
        session.sendError(Response.BAD_REQUEST, null);
    }

    private Response customHandler(RequestHandler rh, String[] nodes, boolean proxied) throws IOException, NoSuchElementException {
        if (proxied) {
            return rh.proxied();
        }

        ArrayList<Future<Boolean>> futures = new ArrayList<>();
        for (final String node : nodes) {
            if (node.equals(me)) {
                futures.add(executor.submit(rh.local()));
            } else {
                futures.add(executor.submit(rh.httpClient(clients.get(node))));
            }
        }

        return rh.getResponse(futures);
    }

    private String[] replicas(String id, int count) throws IllegalArgumentException {
        if (count > topology.length) {
            throw new IllegalArgumentException("The from value must be less or equal to the total count of nodes = " + topology.length);
        }
        String[] result = new String[count];
        int i = (id.hashCode() & Integer.MAX_VALUE) % topology.length;
        for (int j = 0; j < count; j++) {
            result[j] = topology[i];
            i = (i + 1) % topology.length;
        }
        return result;
    }

    private String getMethodName(int method) {
        switch (method) {
            case Request.METHOD_GET:
                return "GET";
            case Request.METHOD_PUT:
                return "PUT";
            case Request.METHOD_DELETE:
                return "DELETE";
            default:
                return "UNSUPPORTED " + method;
        }
    }

    @Override
    public synchronized void stop() {
        super.stop();
        executor.shutdown();
        log.info("Server " + port + " is stopped");
    }
}