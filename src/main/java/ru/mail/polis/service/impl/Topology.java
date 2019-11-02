package ru.mail.polis.service.impl;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Topology {
    private final List<String> nodes;
    private final String me;

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

    Boolean isMe(final String node) {
        return me.equals(node);
    }

    String getId() {
        return this.me;
    }

    Set<String> getNodes() {
        return new HashSet<>(this.nodes);
    }

    /**
     * Get the clusters ids where the replicas will be created.
     *
     * @param count the amount of replicas
     * @param key key id
     * @return ids of the clusters to create replicas
     */
    String[] replicas(final int count, @NotNull final ByteBuffer key) {
        final String[] res = new String[count];
        int index = (key.hashCode() & Integer.MAX_VALUE) % nodes.size();
        for (int j = 0; j < count; j++) {
            res[j] = nodes.get(index);
            index = (index + 1) % nodes.size();
        }
        return res;
    }
}
