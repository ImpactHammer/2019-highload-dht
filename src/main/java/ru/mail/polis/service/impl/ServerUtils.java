package ru.mail.polis.service.impl;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import org.apache.http.client.utils.URIBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.impl.RocksUtils;
import ru.mail.polis.dao.impl.TimestampRecord;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static ru.mail.polis.service.impl.AsyncHttpServer.badRequest;

class ServerUtils {

    private final DAO dao;
    Topology topology;
    private final Executor workerThreads;

    private List<TimestampRecord> records;
    private CompletableFuture<Void> allFutures;
    private int numberRequestsAccepted;
    private final Object syncIncrement = new Object();

    private final Logger log = LogManager.getLogger("default");

    private final Map<String, HttpClient> clientMap;
    private final List<Integer> successCodes = new ArrayList<>(Arrays.asList(200, 201, 202, 404));

    public static String MESSAGE_WRONG_METHOD = "Wrong method";

    ServerUtils(DAO dao, Topology topology, Executor workers) {
        this.topology = topology;
        this.workerThreads = workers;
        this.dao = dao;

        this.clientMap = new HashMap<>();
        for (final String nodeName : topology.getAll()) {
            if (!topology.isMe(nodeName) && !clientMap.containsKey(nodeName)) {
                clientMap.put(nodeName, new HttpClient(new ConnectionString(nodeName + "?timeout=100"), new String[0]));
            }
        }
    }

    private int quorum(final int from) {
        return from % 2 == 1 ? from / 2 + 1 : from / 2;
    }

    void executeAsync(Executor workers, @NotNull final HttpSession session, @NotNull final AsyncHttpServer.Action action) {
        workers.execute(() -> {
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

    void processDirect(ByteBuffer key, final String replicas,
                       final Request request, final HttpSession session, final Topology topology) throws URISyntaxException, IOException {
        int ack, from;
        if (replicas == null) {
            final int clusterSize = topology.getAll().size();
            ack = quorum(clusterSize);
            from = clusterSize;
        } else {
            @SuppressWarnings("StringSplitter")
            final String[] replicasSplitted = replicas.split("/");
            ack = Integer.parseInt(replicasSplitted[0]);
            from = Integer.parseInt(replicasSplitted[1]);
        }

        if (ack < 1 || ack > from) {
            executeAsync(this.workerThreads, session, () -> badRequest(request));
            return;
        }

        final int fromNode = topology.primaryFor(key);

        URIBuilder uriBuilder = new URIBuilder(request.getURI());
        uriBuilder.addParameter("proxied", "true");

        String newURI = uriBuilder.build().toString();
        newURI = newURI.replace("%2F", "/");

        records = new ArrayList<>();
        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        numberRequestsAccepted = 0;
        for (int i = 0; i < from; i++) {
            final int idxNode = (fromNode + i) % topology.getAll().size();
            final String nodeName = topology.getNode(idxNode);
            if (topology.isMe(nodeName)) {
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
            session.sendError(Response.GATEWAY_TIMEOUT, "Not Enough Replicas");
            return;
        }

        final TimestampRecord latestRecord = TimestampRecord.latestOf(records);
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                if (latestRecord == null || latestRecord.getValue() == null) {
                    session.sendError(Response.NOT_FOUND, "Key not found");
                    return;
                }
                session.sendResponse(new Response(Response.OK, latestRecord.getValue()));
                break;
            case Request.METHOD_PUT:
                session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
                break;
            case Request.METHOD_DELETE:
                session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
                break;
            default:
                break;
        }
    }

    private Void handleRemote(final String nodeName, final Request request, final String newURI, final int ack) {
        final HttpClient client = clientMap.get(nodeName);
        final List<String> headerList = new ArrayList<>();
        for (final String h : request.getHeaders()) {
            if (h != null) {
                headerList.add(h);
            }
        }
        String[] headers = new String[headerList.size()];
        for (int idxHeader = 0; idxHeader < headerList.size(); idxHeader++) {
            headers[idxHeader] = headerList.get(idxHeader);
        }
        final Request newRequest = client.createRequest(request.getMethod(), newURI, headers);

        final byte[] tmp = request.getBody();
        if (tmp != null) {
            newRequest.setBody(tmp.clone());
        }

        Response response = null;
        try {
            response = client.invoke(newRequest);
        } catch (InterruptedException | PoolException | IOException | HttpException e) {
            log.debug(e.getMessage());
        }
        if (response != null && successCodes.contains(response.getStatus())) {
            final TimestampRecord record = TimestampRecord.fromByteArray(response.getBody());
            records.add(record);

            synchronized (syncIncrement) {
                numberRequestsAccepted++;
                if (numberRequestsAccepted >= ack) {
                    allFutures.complete(null);
                }
            }
        }
        return null;
    }

    private Void handleLocal(final Request request, final ByteBuffer key, final HttpSession session, final int ack) {
        byte[] body = null;
        boolean knownMethod = true;
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                try {
                    ByteBuffer value = dao.get(key);
                    body = RocksUtils.toArray(value);
                } catch (NoSuchElementException | IOException e) {
                    log.debug(e.getMessage());
                }
                if (body == null) {
                    records.add(null);
                } else {
                    records.add(TimestampRecord.fromByteArray(body));
                }
                break;
            case Request.METHOD_PUT:
                final TimestampRecord r = new TimestampRecord(request.getBody());
                try {
                    body = r.toByteArray();
                    if (body != null) {
                        dao.upsert(key, ByteBuffer.wrap(body));
                    }
                } catch (IOException e) {
                    log.debug(e.getMessage());
                }
                records.add(TimestampRecord.fromByteArray(body));
                break;
            case Request.METHOD_DELETE:
                try {
                    dao.remove(key);
                    body = new TimestampRecord(null).toByteArray();
                    dao.upsert(key, ByteBuffer.wrap(body));
                } catch (IOException e) {
                    log.debug(e.getMessage());
                }
                records.add(TimestampRecord.fromByteArray(body));
                break;
            default:
                try {
                    session.sendError(Response.METHOD_NOT_ALLOWED, MESSAGE_WRONG_METHOD);
                } catch (IOException e) {
                    log.debug(e.getMessage());
                }
                knownMethod = false;
        }
        if (knownMethod) {
            synchronized (syncIncrement) {
                numberRequestsAccepted++;
                if (numberRequestsAccepted >= ack) {
                    allFutures.complete(null);
                }
            }
        }
        return null;
    }
}
