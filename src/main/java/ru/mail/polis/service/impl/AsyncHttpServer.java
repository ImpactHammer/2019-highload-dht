package ru.mail.polis.service.impl;

import com.google.common.base.Charsets;
import one.nio.http.*;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import org.apache.http.client.utils.URIBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.impl.RocksUtils;
import ru.mail.polis.dao.impl.TimestampRecord;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

public class AsyncHttpServer extends HttpServer implements Service {
    private final Topology topology;
    @NotNull
    private final DAO dao;
    @NotNull
    private final Executor workerThreads;
    private final Logger log = LogManager.getLogger("default");

    private final HashMap<String, HttpClient> clientMap;
    private List<Integer> successCodes = new ArrayList<>(Arrays.asList(200, 201, 202, 404));

    private List<TimestampRecord> records;
    private CompletableFuture<Void> allFutures;
    private int nRequestsAccepted;
    private final Object syncIncrement = new Object();

    /**
     * Constructor.
     *
     * @param port - network port
     * @param dao - DAO instance
     * @param workers - executor
     */
    public AsyncHttpServer(final int port, @NotNull final DAO dao,
                           @NotNull final Executor workers,
                           Topology topology) throws IOException {
        super(from(port));
        this.dao = dao;
        this.workerThreads = workers;
        this.topology = topology;

        this.clientMap = new HashMap<>();
        for (String nodeName : topology.getAll()) {
            if (!topology.isMe(nodeName) && !clientMap.containsKey(nodeName)) {
                clientMap.put(nodeName, new HttpClient(new ConnectionString(nodeName + "?timeout=100"), new String[0]));
            }
        }
    }

    private static HttpServerConfig from(final int port) {
        final AcceptorConfig ac = new AcceptorConfig();
        ac.port = port;
        ac.reusePort = true;
        ac.deferAccept = true;

        final HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ac};
        return config;
    }

    @Override
    public HttpSession createSession(final Socket socket) {
        return new StorageSession(socket, this);
    }

    private int quorum(int from) {
        return (from % 2 == 1 ? from/2 + 1 : from / 2);
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

    private Void handleRemote(String nodeName, Request request, String newURI, int ack) {
        HttpClient client = clientMap.get(nodeName);
        List<String> headerList = new ArrayList<>();
        for (String h : request.getHeaders()) {
            if (h != null) {
                headerList.add(h);
            }
        }
        String[] headers = new String[headerList.size()];
        for (int i_header = 0; i_header < headerList.size(); i_header++) {
            headers[i_header] = headerList.get(i_header);
        }
//                    String uri = request.getURI();
        Request newRequest = client.createRequest(request.getMethod(), newURI, headers);

        byte[] tmp = request.getBody();
        if (tmp != null) {
            newRequest.setBody(tmp.clone());
        }

        Response response = null;
        try {
            response = client.invoke(newRequest);
        } catch (Exception e) {
//                        log.debug(e.getMessage());
        }
        if (response != null && successCodes.contains(response.getStatus())) {
            TimestampRecord record = TimestampRecord.fromByteArray(response.getBody());
            records.add(record);

            synchronized (syncIncrement) {
                nRequestsAccepted++;
                if (nRequestsAccepted >= ack) {
                    allFutures.complete(null);
                }
            }
        }
        return null;
    }

    private Void handleLocal(Request request, ByteBuffer key, HttpSession session, int ack) {
        byte[] body = null;
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                try {
                    ByteBuffer value = null;
                    try {
                        value = dao.get(key);
                    } catch (Exception e) {
                        log.debug(e.getMessage());
                    }
                    if (value != null) {
                        body = RocksUtils.toArray(value);
                    }
                } catch (NoSuchElementException e) {
                }
                if (body != null) {
                    records.add(TimestampRecord.fromByteArray(body));
                } else {
                    records.add(null);
                }
                break;
            case Request.METHOD_PUT:
                TimestampRecord r = new TimestampRecord(request.getBody());
                try {
                    body = r.toByteArray();
                } catch (Exception e) {
                    log.debug(e.getMessage());
                }
                if (body != null) {
                    try {
                        dao.upsert(key, ByteBuffer.wrap(body));
                    } catch (IOException e) {
                        log.debug(e.getMessage());
                    }
                }
                records.add(TimestampRecord.fromByteArray(body));
                break;
            case Request.METHOD_DELETE:
                try {
                    dao.remove(key);
                } catch (IOException e) {
                    log.debug(e.getMessage());
                }
                try {
                    body = new TimestampRecord(null).toByteArray();
                } catch (IOException e) {
                    log.debug(e.getMessage());
                }
                try {
                    dao.upsert(key, ByteBuffer.wrap(body));
                } catch (IOException e) {
                    log.debug(e.getMessage());
                }
                records.add(TimestampRecord.fromByteArray(body));
                break;
            default:
                try {
                    session.sendError(Response.METHOD_NOT_ALLOWED, "Wrong method");
                } catch (IOException e) {
                    log.debug(e.getMessage());
                }
                return null;
        }
        synchronized (syncIncrement) {
            nRequestsAccepted++;
            if (nRequestsAccepted >= ack) {
                allFutures.complete(null);
            }
        }
        return null;
    }

    /**
     * Single element request handler.
     *
     * @param id - element key
     * @param request - http request
     * @param session - http session
     */
    @Path("/v0/entity")
    public void entity(@Param("id") final String id, @Param("replicas") final String replicas,
                       @Param("proxied") final String proxied,
                       @NotNull final Request request, final HttpSession session) throws URISyntaxException
//            throws IOException, InterruptedException, HttpException, PoolException,
//            URISyntaxException, ClassNotFoundException
    {
        if (id == null || id.isEmpty()) {
            executeAsync(session, () -> badRequest(request));
            return;
        }


//        try {
//            if (request.getMethod() == 1) {
//                    session.sendResponse(new Response(Response.OK, Response.EMPTY));
//            } else {
//                session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
//            }
//        } catch (IOException e) {
//            log.debug(e.getMessage());
//        }
//
//        if (true) {
//            return;
//        }

        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));


        if (proxied == null || proxied.isEmpty()) {
            int ack, from;
            if (replicas != null) {
                @SuppressWarnings("StringSplitter")
                String[] replicasSplitted = replicas.split("/");
                ack = Integer.parseInt(replicasSplitted[0]);
                from = Integer.parseInt(replicasSplitted[1]);
            } else {
                int clusterSize = topology.getAll().size();
                ack = quorum(clusterSize);
                from = clusterSize;
            }

            if (ack < 1 || ack > from) {
                executeAsync(session, () -> badRequest(request));
                return;
            }

            int fromNode = topology.primaryFor(key);


            URIBuilder uriBuilder = null;
            try {
                uriBuilder = new URIBuilder(request.getURI());
            } catch (URISyntaxException e) {
                log.debug(e.getMessage());
            }
            assert uriBuilder != null;
            uriBuilder.addParameter("proxied", "true");
            String newURI = null;
            try {
                newURI = uriBuilder.build().toString();
                newURI = newURI.replace("%2F", "/");

            } catch (URISyntaxException e) {
                log.debug(e.getMessage());
            }

            records = new ArrayList<>();
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            nRequestsAccepted = 0;
            for (int i = 0; i < from; i++) {
                int iNode = (fromNode + i) % topology.getAll().size();
                String nodeName = topology.getNode(iNode);
                if (topology.isMe(nodeName)){
                    futures.add(CompletableFuture.supplyAsync(() -> handleLocal(request, key, session, ack)));
                } else {
                    String finalNewURI = newURI;
                    futures.add(CompletableFuture.supplyAsync(() -> handleRemote(nodeName, request, finalNewURI, ack)));
                }
            }
            allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
            try {
                allFutures.get();
            } catch (InterruptedException | ExecutionException e) {
                log.debug(e.getMessage());
            }

            if (records.size() < ack) {
                try {
                    session.sendError(Response.GATEWAY_TIMEOUT, "Not Enough Replicas");
                } catch (IOException e) {
                    log.debug(e.getMessage());
                }
                return;
            }

            TimestampRecord latestRecord = TimestampRecord.latestOf(records);
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    if (latestRecord == null || latestRecord.getValue() == null) {
                        try {
                            session.sendError(Response.NOT_FOUND, "Key not found");
                        } catch (IOException e) {
                            log.debug(e.getMessage());
                        }
                        return;
                    }
                    try {
                        session.sendResponse(new Response(Response.OK, latestRecord.getValue()));
                    } catch (IOException e) {
                        log.debug(e.getMessage());
                    }
                    break;
                case Request.METHOD_PUT:
                    try {
                        session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
                    } catch (IOException e) {
                        log.debug(e.getMessage());
                    }
                    break;
                case Request.METHOD_DELETE:
                    try {
                        session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
                    } catch (IOException e) {
                        log.debug(e.getMessage());
                    }
                    break;
                default:
                    break;
            }
            return;
        }

        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    executeAsync(session, () -> getMethodWrapper(key));
                    break;
                case Request.METHOD_PUT:
                    executeAsync(session, () -> putMethodWrapper(key, request));
                    break;
                case Request.METHOD_DELETE:
                    executeAsync(session, () -> deleteMethodWrapper(key));
                    break;
                default:
                    session.sendError(Response.METHOD_NOT_ALLOWED, "Wrong method");
                    break;
            }
        } catch (IOException e) {
            try {
                session.sendError(Response.INTERNAL_ERROR, e.getMessage());
            } catch (IOException ex) {
                log.debug(ex.getMessage());
            }
        }
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        final Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    void executeAsync(@NotNull final HttpSession session, @NotNull final Action action) {
        workerThreads.execute(() -> {
            try {
                session.sendResponse(action.act());
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, e.getMessage());
                } catch (IOException ex) {
                    log.debug("Can't send error response");
                }
            }
        });
    }

    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }

    /**
     * Multiple element request handler.
     *
     * @param start - start key
     * @param end - end key
     * @param request - http request
     * @param session - http session
     */
    @Path("/v0/entities")
    public void entities(@Param("start") final String start,
                         @Param("end") final String end,
                         @NotNull final Request request, @NotNull final HttpSession session) throws IOException {

        if (start == null || start.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "No start");
            return;
        }

        if (request.getMethod() != Request.METHOD_GET) {
            session.sendError(Response.METHOD_NOT_ALLOWED, "Wrong method");
            return;
        }

        final boolean notEndSpecified = end == null || end.isEmpty();

        try {
            final Iterator<Record> records =
                    dao.range(ByteBuffer.wrap(start.getBytes(StandardCharsets.UTF_8)),
                            notEndSpecified ? null : ByteBuffer.wrap(end.getBytes(StandardCharsets.UTF_8)));
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            session.sendError(Response.INTERNAL_ERROR, e.getMessage());
        }
    }

    @NotNull
    Response badRequest(Request request) {
        return new Response(Response.BAD_REQUEST, Response.EMPTY);
    }

    @NotNull
    private Response getMethodWrapper(final ByteBuffer key) {
        Response response;
        try {
            ByteBuffer value = null;
            try {
                value = dao.get(key);
            } catch (IOException e) {
                log.debug(e.getMessage());
            }
            final byte[] body = RocksUtils.toArray(value);

            response = new Response(Response.OK, body);
            return response;
        }
        catch (NoSuchElementException e) {
            response = new Response(Response.NOT_FOUND, "Key not found".getBytes(Charsets.UTF_8));
            return response;
        }
    }

    @NotNull
    private Response putMethodWrapper(final ByteBuffer key, final Request request) {
        byte[] body = null;
        try {
            body = new TimestampRecord(request.getBody()).toByteArray();
        } catch (IOException e) {
            log.debug(e.getMessage());
        }
        if (body != null) {
            try {
                dao.upsert(key, ByteBuffer.wrap(body));
            } catch (IOException e) {
                log.debug(e.getMessage());
            }
        }
        return new Response(Response.CREATED, Response.EMPTY);
    }

    @NotNull
    private Response deleteMethodWrapper(final ByteBuffer key) {
        try {
            dao.remove(key);
        } catch (IOException e) {
            log.debug(e.getMessage());
        }
        try {
            dao.upsert(key, ByteBuffer.wrap(new TimestampRecord(null).toByteArray()));
        } catch (IOException e) {
            log.debug(e.getMessage());
        }
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }
}
