package com.vmware;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.vmware.dcm.Model;
import com.vmware.generated.Tables;
import com.vmware.generated.tables.records.DatabaseRecord;
import com.vmware.generated.tables.records.RangeRecord;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.using;

public class ReplicaPlacement {
    private static final int DEFAULT_NUM_REPLICAS = 3;
    private static final Integer AUTOGENERATED_KEY = null;
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
     * Create a database with defaults for num_replicas and constraints
     */
    public void addDatabase(final String name) {
        addDatabase(name, DEFAULT_NUM_REPLICAS, "");
    }

    /*
     * Create a database with specified num_replicas and constraints. The database will be mapped
     * to a single range by default.
     */
    public void addDatabase(final String name, final int numReplicas, final String constraintsJson) {
        final DatabaseRecord databaseRecord = conn.newRecord(Tables.DATABASE);
        databaseRecord.setName(name);
        databaseRecord.setNumReplicas(numReplicas);
        databaseRecord.store();

        // Start with only 1 range
        final RangeRecord rangeRecord = conn.newRecord(Tables.RANGE);
        rangeRecord.setDatabaseId(databaseRecord.getId());
        rangeRecord.store();

        // Parse the set of constraints according to their scope
        // https://www.cockroachlabs.com/docs/v21.1/configure-replication-zones#scope-of-constraints
        final JsonElement jsonElement = JsonParser.parseString(constraintsJson);
        final List<List<String>> allConstraints = new ArrayList<>();

        // "All replicas" constraint scope
        // Constraints specified using JSON array syntax apply to all replicas in every
        // range that's part of the replication zone.
        if (jsonElement.isJsonArray()) {
            final JsonArray jsonArray = jsonElement.getAsJsonArray();
            final List<String> constraintsAsList = new ArrayList<>(jsonArray.size());
            for (int i = 0; i < jsonArray.size(); i++) {
                constraintsAsList.add(jsonArray.get(i).getAsString());
            }
            for (int i = 0; i < numReplicas; i++) {
                allConstraints.add(constraintsAsList);
            }
        }
        // "Per-Replica" constraint scope
        // Multiple lists of constraints can be provided in a JSON object, mapping each list
        // of constraints to an integer number of replicas in each range that the constraints should apply to.
        else if (jsonElement.isJsonObject()) {
            final JsonObject jsonObject = jsonElement.getAsJsonObject();
            for (final Map.Entry<String, JsonElement> entry: jsonObject.entrySet()) {
                final String listOfConstraints = entry.getKey();
                final int numberOfReplicas = entry.getValue().getAsInt();
                for (int i = 0; i < numberOfReplicas; i++) {
                    allConstraints.add(arrayToConstraints(JsonParser.parseString(listOfConstraints)));
                }
            }
        }

        // The total number of replicas constrained cannot be greater than the total number of replicas for the zone
        // (num_replicas). However, if the total number of replicas constrained is less than the total
        // number of replicas for the zone, the non-constrained replicas will be allowed on any nodes/stores.
        assert allConstraints.size() <= numReplicas;
        for (int i = 0; i < numReplicas; i++) {
            if (allConstraints.size() > i) {
                addReplica(rangeRecord.getId(), allConstraints.get(i));
            } else {
                addReplica(rangeRecord.getId(), Collections.emptyList());
            }
        }
    }

    /*
     * Run the DCM model to compute a placement decision for new replicas
     */
    public Result<? extends Record> placeReplicas() {
        model.updateData();
        final Result<? extends Record> solution = model.solve(Tables.PENDING_REPLICAS.getName());
        solution.forEach(
            r -> conn.update(Tables.REPLICA)
                .set(Tables.REPLICA.CURRENT_NODE, r.get(Tables.REPLICA.CONTROLLABLE__NODE))
                .set(Tables.REPLICA.STATUS, "running")
                .where(Tables.REPLICA.ID.eq(r.get(Tables.REPLICA.ID)))
                .execute()
        );
        return solution;
    }

    public void printState() {
        for (final Table<?> table: List.of(Tables.DATABASE, Tables.RANGE, Tables.NODE, Tables.NODE_LABEL,
                                           Tables.REPLICA,
                                           Tables.REPLICA_CONSTRAINT, Tables.REPLICA_TO_NODE_CONSTRAINT_MATCHING,
                                           Tables.NODE_AZS, Tables.PENDING_REPLICAS)) {
            System.out.println("---" + table.getName() + "---");
            System.out.println(conn.fetch(table));
        }
    }

    public Result<ReplicaRecord> getReplicaState() {
        return conn.fetch(Tables.REPLICA);
    }

    /*
     * Add a replica to the state database with arguments that correspond to the 'constraints' field here:
     * https://www.cockroachlabs.com/docs/v21.1/configure-replication-zones#types-of-constraints
     *
     * TODO: Add voting/non-voting replica distinction later
     * TODO: Support per-replica constraint syntax as well
     */
    private void addReplica(final int rangeId, final List<String> constraints) {
        ReplicaRecord replicaRecord = conn.insertInto(Tables.REPLICA)
                .values(AUTOGENERATED_KEY, rangeId, "pending", null, null)
                .returning(Tables.REPLICA.ID)
                .fetchOne();
        constraints.stream().filter(e -> e.startsWith("+"))
                .map(l -> toKeyValuePair(l.substring(1), "="))
                .forEach(kvPair -> addRequiredReplicaConstraint(replicaRecord.getId(),
                        rangeId, kvPair.key, kvPair.value));
        constraints.stream().filter(e -> e.startsWith("-"))
                .map(l -> toKeyValuePair(l.substring(1), "="))
                .forEach(kvPair -> addProhibitedReplicaConstraint(replicaRecord.getId(),
                        rangeId, kvPair.key, kvPair.value));
    }

    private List<String> arrayToConstraints(final JsonElement jsonElement) {
        final JsonArray jsonArray = jsonElement.getAsJsonArray();
        final List<String> constraintsAsList = new ArrayList<>(jsonArray.size());
        for (int i = 0; i < jsonArray.size(); i++) {
            constraintsAsList.add(jsonArray.get(i).getAsString());
        }
        return constraintsAsList;
    }

    private void addNodeLabel(final int nodeId, final String labelKey, final String labelValue) {
        conn.insertInto(Tables.NODE_LABEL)
                .values(nodeId, labelKey, labelValue)
                .execute();
    }

    private void addRequiredReplicaConstraint(final int replicaId, final int rangeId, final String labelKey,
                                              final String labelValue) {
        conn.insertInto(Tables.REPLICA_CONSTRAINT)
            .values(replicaId, rangeId, "required", labelKey, labelValue)
            .execute();
    }

    private void addProhibitedReplicaConstraint(final int replicaId, final int rangeId, final String labelKey,
                                                final String labelValue) {
        conn.insertInto(Tables.REPLICA_CONSTRAINT)
            .values(replicaId, rangeId, "prohibited", labelKey, labelValue)
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
