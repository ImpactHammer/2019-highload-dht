package ru.mail.polis.service.impl;

import com.google.common.base.Charsets;
import one.nio.http.*;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;

import static one.nio.http.Response.METHOD_NOT_ALLOWED;
import static one.nio.http.Response.INTERNAL_ERROR;
import static one.nio.http.Response.BAD_REQUEST;

public class AsyncHttpServer extends HttpServer implements Service {
    @NotNull
    private final DAO dao;
    @NotNull
    private final Executor workerThreads;

    public AsyncHttpServer(final int port, @NotNull final DAO dao,
                           @NotNull final Executor workers) throws IOException {
        super(from(port));
        this.dao = dao;
        this.workerThreads = workers;
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

    @Path("/v0/entity")
    public void entity(@Param("id") final String id,
                        @NotNull final Request request, HttpSession session) throws IOException {
        if (id == null || id.isEmpty()) {
            executeAsync(session, () -> badRequest());
            return;
        }
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
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
                    session.sendError(METHOD_NOT_ALLOWED, "Wrong method");
            }
        } catch (IOException e) {
            session.sendError(INTERNAL_ERROR, e.getMessage());
        }
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
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
//                    ex.printStackTrace();
                }
            }
        });
    }

    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }

    @Path("/v0/entities")
    public void entities(@Param("start") final String start,
                          @Param("end") String end,
                          @NotNull final Request request, @NotNull final HttpSession session) throws IOException {

        if (start == null || start.isEmpty()) {
            session.sendError(BAD_REQUEST, "No start");
            return;
        }

        if (request.getMethod() != Request.METHOD_GET) {
            session.sendError(METHOD_NOT_ALLOWED, "Wrong method");
            return;
        }

        if (end != null && end.isEmpty()) {
            end = null;
        }

        try {
            final Iterator<Record> records =
                    dao.range(ByteBuffer.wrap(start.getBytes(StandardCharsets.UTF_8)),
                            end == null ? null : ByteBuffer.wrap(end.getBytes(StandardCharsets.UTF_8)));
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            session.sendError(INTERNAL_ERROR, e.getMessage());
        }
    }

    @NotNull
    Response badRequest() {
        return new Response(Response.BAD_REQUEST, Response.EMPTY);
    }

    @NotNull
    private Response getMethodWrapper(final ByteBuffer key) throws IOException {
        Response response;
        try {
            final ByteBuffer value = dao.get(key).duplicate();
            byte[] body = new byte[value.remaining()];
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
        byte[] body = request.getBody();
        if (body != null) {
            dao.upsert(key, ByteBuffer.wrap(body));
        }
        return new Response(Response.CREATED, Response.EMPTY);
    }

    @NotNull
    private Response deleteMethodWrapper(final ByteBuffer key) throws IOException {
        dao.remove(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }
}
