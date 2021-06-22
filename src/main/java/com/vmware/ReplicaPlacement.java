package com.vmware;

import com.vmware.dcm.Model;
import com.vmware.generated.Tables;
import com.vmware.generated.tables.records.ReplicaRecord;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.using;

public class ReplicaPlacement {
    private final DSLContext conn;
    private final Model model;

    ReplicaPlacement(final List<String> constraints) {
        conn = setup();
        model = Model.build(conn, constraints);
    }

    /*
     * Add a node to the state database with arguments that correspond to the --locality, --attrs, and attrs
     * field of the --store field, described here:
     * https://www.cockroachlabs.com/docs/v21.1/configure-replication-zones#descriptive-attributes-assigned-to-nodes
     */
    public void addNodeWithAttributes(final int nodeId, final List<String> localityLabels,
                                      final List<String> nodeCapabilityLabels,
                                      final List<String> storeCapabilityLabels) {
        conn.insertInto(Tables.NODE)
                .values(nodeId)
                .execute();
        localityLabels.stream().map(l -> toKeyValuePair(l, "="))
                     .forEach(kvPair -> addNodeLabel(nodeId, kvPair.key, kvPair.value));
        nodeCapabilityLabels.stream().map(l -> toKeyValuePair(l, ":"))
                     .forEach(kvPair -> addNodeLabel(nodeId, kvPair.key, kvPair.value));
        storeCapabilityLabels.stream().map(l -> toKeyValuePair(l, ":"))
                     .forEach(kvPair -> addNodeLabel(nodeId, kvPair.key, kvPair.value));
    }

    /*
     * Add a replica to the state database with arguments that correspond to the 'constraints' field here:
     * https://www.cockroachlabs.com/docs/v21.1/configure-replication-zones#types-of-constraints
     *
     * TODO: Add voting/non-voting replica distinction later
     * TODO: Support per-replica constraint syntax as well
     */
    public void addReplica(final int replicaId, final int shardId, final List<String> constraints) {
        conn.insertInto(Tables.REPLICA)
                .values(replicaId, shardId, "Pending", null)
                .execute();
        constraints.stream().filter(e -> e.startsWith("+"))
                .map(l -> toKeyValuePair(l.substring(1), "="))
                .forEach(kvPair -> addRequiredReplicaConstraint(replicaId, shardId, kvPair.key, kvPair.value));
        constraints.stream().filter(e -> e.startsWith("-"))
                .map(l -> toKeyValuePair(l.substring(1), "="))
                .forEach(kvPair -> addProhibitedReplicaConstraint(replicaId, shardId, kvPair.key, kvPair.value));
    }

    /*
     * Run the DCM model to compute a placement decision for new replicas
     */
    public Result<? extends Record> placeReplicas() {
        model.updateData();
        return model.solve(Tables.REPLICA.getName());
    }

    public void printState() {
        for (final Table<?> table: List.of(Tables.NODE, Tables.NODE_LABEL, Tables.REPLICA, Tables.REPLICA_CONSTRAINT,
                                           Tables.REPLICA_TO_NODE_CONSTRAINT_MATCHING)) {
            System.out.println("---" + table.getName() + "---");
            System.out.println(conn.fetch(table));
        }
    }

    public Result<ReplicaRecord> getReplicaState() {
        return conn.fetch(Tables.REPLICA);
    }

    private void addNodeLabel(final int nodeId, final String labelKey, final String labelValue) {
        conn.insertInto(Tables.NODE_LABEL)
                .values(nodeId, labelKey, labelValue)
                .execute();
    }

    private void addRequiredReplicaConstraint(final int replicaId, final int shardId, final String labelKey,
                                              final String labelValue) {
        conn.insertInto(Tables.REPLICA_CONSTRAINT)
            .values(replicaId, shardId, "Required", labelKey, labelValue)
            .execute();
    }

    private void addProhibitedReplicaConstraint(final int replicaId, final int shardId, final String labelKey,
                                                final String labelValue) {
        conn.insertInto(Tables.REPLICA_CONSTRAINT)
            .values(replicaId, shardId, "Prohibited", labelKey, labelValue)
            .execute();
    }

    /*
     * Sets up an in-memory database using the schema.sql file.
     */
    private DSLContext setup() {
        try {
            final DSLContext using = using("jdbc:h2:mem:");
            final InputStream resourceAsStream = this.getClass().getResourceAsStream("/schema.sql");
            final BufferedReader reader =
                    new BufferedReader(new InputStreamReader(resourceAsStream, StandardCharsets.UTF_8));
            final String schemaAsString = reader
                    .lines()
                    .filter(line -> !line.startsWith("--")) // remove SQL comments
                    .collect(Collectors.joining("\n"));
            final List<String> semiColonSeparated = Arrays.stream(schemaAsString.split(";"))
                    .filter(e -> !e.isEmpty())
                    .map(String::trim)
                    .collect(Collectors.toList());
            reader.close();
            semiColonSeparated.forEach(using::execute);
            return using;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private KVPair toKeyValuePair(final String labelString, final String splitter) {
        final String[] split = labelString.split(splitter);
        if (split.length == 2) {
            final String key = split[0].trim();
            final String value = split[1].trim();
            return new KVPair(key, value);
        } else if (split.length == 1) {
            final String key = split[0].trim();
            return new KVPair(key, null);
        } else {
            throw new IllegalArgumentException(String.format("Unexpected labelString: %s", labelString));
        }
    }

    private static class KVPair {
        final String key;
        final String value;

        public KVPair(final String key, final String value) {
            this.key = key;
            this.value = value;
        }
    }
}
