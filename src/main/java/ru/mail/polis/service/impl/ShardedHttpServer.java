package ru.mail.polis.service.impl;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

public class ShardedHttpServer extends AsyncHttpServer {
    private final Topology topology;
    private final Map<String, HttpClient> pool;

    /**
     * Constructor.
     *
     * @param port - network port
     * @param dao - DAO instance
     * @param executor - executor
     * @param topology - Topology describing cluster
     */
    public ShardedHttpServer(
            final int port,
            @NotNull final DAO dao,
            @NotNull final Executor executor,
            final Topology topology
    ) throws IOException {
        super(port, dao, executor);
        this.topology = topology;

        pool = new HashMap<>();
        for (final String node : topology.getAll()) {
            if (topology.isMe(node)) {
                continue;
            }

            assert !pool.containsKey(node);
            pool.put(node, new HttpClient(new ConnectionString(node + "?timeout=100")));
        }
    }

    @Override
    @Path("/v0/entity")
    public void entity(@Param("id") final String id,
                       @NotNull final Request request, final HttpSession session) throws IOException {
        if (id == null || id.isEmpty()) {
            executeAsync(session, () -> badRequest());
            return;
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final String primary = topology.primaryFor(key);
        if (topology.isMe(primary)) {
            super.entity(id, request, session);
        } else {
            executeAsync(session, () -> proxy(primary, request));
        }
    }

    private Response proxy(final String node, final Request request) throws IOException {
        assert !topology.isMe(node);
        try {
            return pool.get(node).invoke(request);
        } catch (InterruptedException | PoolException | HttpException e) {
            e.printStackTrace();
            throw new IOException("Can't proxy", e);
        }
    }
}
