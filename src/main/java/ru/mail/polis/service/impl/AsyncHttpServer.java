package ru.mail.polis.service.impl;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import one.nio.http.HttpClient;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class AsyncHttpServer extends HttpServer implements Service {
    @NotNull
    private final DAO dao;
    @NotNull
    private final Executor workerThreads;
    private final Topology nodes;
    private final int clusterSize;
    private final Map<String, HttpClient> clusterClients;
    private static final String PROXY_HEADER = "X-OK-Proxy: True";
    private final RF defaultRF;
    private final Logger log = LogManager.getLogger("default");

    /**
     * Constructor.
     *
     * @param port - network port
     * @param dao - DAO instance
     * @param nodes - cluster topology
     */
    public AsyncHttpServer(final int port, @NotNull final DAO dao,
                           @NotNull final Topology nodes) throws IOException {
        super(from(port));
        this.dao = dao;

        this.workerThreads = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryBuilder().setNameFormat("worker").build());

        final Map<String, HttpClient> clients = new HashMap<>();
        for (final String it : nodes.getNodes()) {
            if (!nodes.isMe(it) && !clients.containsKey(it)) {
                clients.put(it, new HttpClient(new ConnectionString(it + "?timeout=100")));
            }
        }

        this.nodes = nodes;
        this.clusterClients = clients;
        this.defaultRF = new RF(nodes.getNodes().size() / 2 + 1, nodes.getNodes().size());
        this.clusterSize = nodes.getNodes().size();
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

    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

    /**
     * Single element request handler.
     *
     * @param id - element key
     * @param request - http request
     * @param session - http session
     */
    @Path("/v0/entity")
    public void entity(@Param("id") final String id,
                       @NotNull final Request request, final HttpSession session) throws IOException {
        if (id == null || id.isEmpty()) {
            executeAsync(session, () -> badRequest());
            return;
        }
        boolean proxied = false;
        if (request.getHeader(PROXY_HEADER) != null) {
            proxied = true;
        }
        final String replicas = request.getParameter("replicas");
        final RF rf = RF.calculateRF(replicas, session, defaultRF, clusterSize);
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final boolean proxiedF = proxied;

        if (proxied || nodes.getNodes().size() > 1) {
            final Coordinators clusterCoordinator = new Coordinators(nodes, clusterClients, dao, proxiedF);
            final String[] replicaClusters = proxied ? new String[]{nodes.getId()} : nodes.replicas(rf.getFrom(), key);
            clusterCoordinator.coordinateRequest(replicaClusters, request, rf.getAck(), session);
        } else {
            try {
                switch (request.getMethod()) {
                    case Request.METHOD_GET:
                        executeAsync(session, () -> getMethodWrapper(key));
                        return;
                    case Request.METHOD_PUT:
                        executeAsync(session, () -> putMethodWrapper(key, request));
                        return;
                    case Request.METHOD_DELETE:
                        executeAsync(session, () -> deleteMethodWrapper(key));
                        return;
                    default:
                        session.sendError(Response.METHOD_NOT_ALLOWED, "Wrong method");
                        return;
                }
            } catch (IOException e) {
                session.sendError(Response.INTERNAL_ERROR, e.getMessage());
            }
        }
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        final Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    private void executeAsync(@NotNull final HttpSession session, @NotNull final Action action) throws IOException {
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
    private Response badRequest() {
        return new Response(Response.BAD_REQUEST, Response.EMPTY);
    }

    @NotNull
    private Response getMethodWrapper(final ByteBuffer key) throws IOException {
        Response response;
        try {
            final ByteBuffer value = dao.get(key).duplicate();
            final byte[] body = new byte[value.remaining()];
            value.get(body);
            response = new Response(Response.OK, body);
            return response;
        }
        catch (NoSuchElementException e) {
            response = new Response(Response.NOT_FOUND, "Key not found".getBytes(Charsets.UTF_8));
            return response;
        }
    }

    @NotNull
    private Response putMethodWrapper(final ByteBuffer key, final Request request) throws IOException {
        dao.upsert(key, ByteBuffer.wrap(request.getBody()));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    @NotNull
    private Response deleteMethodWrapper(final ByteBuffer key) throws IOException {
        dao.remove(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }
}
