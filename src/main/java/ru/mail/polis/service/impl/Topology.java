package ru.mail.polis.service.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Topology {
    private final List<String> nodes;
    final String me;

    /**
     * Constructor.
     *
     * @param nodes - identifiers of all nodes
     * @param me - identifier of this node
     */
    public Topology(final Set<String> nodes, final String me) {
        assert nodes.contains(me);
        this.nodes = new ArrayList<>(nodes);
        this.me = me;
    }

    String primaryFor(final ByteBuffer key) {
        final int hash = key.hashCode();
        final int node = (hash & Integer.MAX_VALUE) % nodes.size();
        return nodes.get(node);
    }

    public Set<String> getAll() {
        return new HashSet<>(nodes);
    }

    Boolean isMe(final String node) {
        return me.equals(node);
    }
}
