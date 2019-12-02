package ru.mail.polis.service.impl;

import com.google.common.base.Charsets;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;


public class AsyncHttpServer extends HttpServer implements Service {
    private final Topology topology;
    @NotNull
    private final DAO dao;

    private ServerUtils serverUtils;

    /**
     * Constructor.
     *
     * @param port    - network port
     * @param dao     - DAO instance
     * @param workers - executor
     */
    public AsyncHttpServer(final int port, @NotNull final DAO dao,
                           @NotNull final Executor workers,
                           Topology topology) throws IOException {
        super(from(port));
        this.dao = dao;
        this.topology = topology;

        this.serverUtils = new ServerUtils(dao, topology, workers);
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
     * @param id      - element key
     * @param request - http request
     * @param session - http session
     */
    @Path("/v0/entity")
    public void entity(@Param("id") final String id, @Param("replicas") final String replicas,
                       @Param("proxied") final String proxied,
                       @NotNull final Request request, final HttpSession session) throws URISyntaxException, IOException {
        if (id == null || id.isEmpty()) {
            this.serverUtils.executeAsync(workers, session, () -> badRequest(request));
            return;
        }

        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));

        if (proxied == null || proxied.isEmpty()) {
            this.serverUtils.processDirect(key, replicas, request, session, topology);
            return;
        }

        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    this.serverUtils.executeAsync(workers, session, () -> getMethodWrapper(key));
                    break;
                case Request.METHOD_PUT:
                    this.serverUtils.executeAsync(workers, session, () -> putMethodWrapper(key, request));
                    break;
                case Request.METHOD_DELETE:
                    this.serverUtils.executeAsync(workers, session, () -> deleteMethodWrapper(key));
                    break;
                default:
                    session.sendError(Response.METHOD_NOT_ALLOWED, ServerUtils.MESSAGE_WRONG_METHOD);
                    break;
            }
        } catch (IOException e) {
            session.sendError(Response.INTERNAL_ERROR, e.getMessage());
        }
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        final Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }


    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }

    /**
     * Multiple element request handler.
     *
     * @param start   - start key
     * @param end     - end key
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
            session.sendError(Response.METHOD_NOT_ALLOWED, ServerUtils.MESSAGE_WRONG_METHOD);
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
    static Response badRequest(final Request request) {
        return new Response(Response.BAD_REQUEST, Response.EMPTY);
    }

    @NotNull
    private Response getMethodWrapper(final ByteBuffer key) {
        Response response;
        ByteBuffer value = null;
        try {
            value = dao.get(key);
            final byte[] body = RocksUtils.toArray(value);

            response = new Response(Response.OK, body);
            return response;
        } catch (NoSuchElementException | IOException e) {
            response = new Response(Response.NOT_FOUND, "Key not found".getBytes(Charsets.UTF_8));
            return response;
        }
    }

    @NotNull
    private Response putMethodWrapper(final ByteBuffer key, final Request request) throws IOException {
        byte[] body = new TimestampRecord(request.getBody()).toByteArray();
        if (body != null) {
            dao.upsert(key, ByteBuffer.wrap(body));
        }
        return new Response(Response.CREATED, Response.EMPTY);
    }

    @NotNull
    private Response deleteMethodWrapper(final ByteBuffer key) throws IOException {
        dao.remove(key);
        dao.upsert(key, ByteBuffer.wrap(new TimestampRecord(null).toByteArray()));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }
}
