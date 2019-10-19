package ru.mail.polis.service.impl;

import com.google.common.base.Charsets;
import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class MyHttpServer extends HttpServer implements Service {
    private DAO dao;

    public MyHttpServer(final int port, @NotNull DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    @Path("/v0/entity")
    public Response entity(@Param("id") final String id,
                           final Request request) {
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    try {
                        final ByteBuffer value = dao.get(key).duplicate();
                        byte[] body = value.array();
                        return new Response(Response.OK, body);
                    }
                    catch (NoSuchElementException e) {
                        return new Response(Response.NOT_FOUND, "Key not found".getBytes(Charsets.UTF_8));
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case Request.METHOD_PUT:
                    dao.upsert(key, ByteBuffer.wrap(request.getBody()));
                    return new Response(Response.CREATED, Response.EMPTY);

                case Request.METHOD_DELETE:
                    dao.remove(key);
                    return new Response(Response.ACCEPTED, Response.EMPTY);

                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
    }

    @Path("/v0/status")
    public Response status() {
            return Response.ok(Response.OK);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    private static HttpServerConfig getConfig(final int port) {
        if (port <= 1024 || port >= 65535) {
            throw new IllegalArgumentException("Invalid port");
        }
        AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        HttpServerConfig httpServerConfig = new HttpServerConfig();
        httpServerConfig.acceptors = new AcceptorConfig[]{acceptor};
        httpServerConfig.selectors = 4;
        return httpServerConfig;
    }
}
