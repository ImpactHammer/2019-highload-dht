package ru.mail.polis.service.impl;

import com.google.common.base.Charsets;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class MyHttpServer extends HttpServer implements Service {
    private final DAO dao;

    public MyHttpServer(final int port, @NotNull final DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    /**
     * @param id Key
     * @param request Http Request
     * @return Http Response
     */
    @Path("/v0/entity")
    public Response entity(@Param("id") final String id,
                           final Request request) throws IOException {
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        Response response = new Response(Response.INTERNAL_ERROR, Response.EMPTY);

            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    response = get(key);
                    break;

                case Request.METHOD_PUT:
                    dao.upsert(key, ByteBuffer.wrap(request.getBody()));
                    response = new Response(Response.CREATED, Response.EMPTY);
                    break;

                case Request.METHOD_DELETE:
                    dao.remove(key);
                    response = new Response(Response.ACCEPTED, Response.EMPTY);
                    break;

                default:
                    response = new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
                    break;
            }
        return response;
    }

    Response get(final ByteBuffer key) throws IOException {
        try {
            final ByteBuffer value = dao.get(key).duplicate();
            final byte[] body = new byte[value.remaining()];
            value.get(body);
            return new Response(Response.OK, body);
        }
        catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, "Key not found".getBytes(Charsets.UTF_8));
        }
    }

    @Path("/v0/status")
    public Response status() {
            return Response.ok(Response.OK);
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        final Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    private static HttpServerConfig getConfig(final int port) {
        if (port <= 1024 || port >= 65535) {
            throw new IllegalArgumentException("Invalid port");
        }
        final AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        final HttpServerConfig httpServerConfig = new HttpServerConfig();
        httpServerConfig.acceptors = new AcceptorConfig[]{acceptor};
        httpServerConfig.selectors = 4;
        return httpServerConfig;
    }
}
