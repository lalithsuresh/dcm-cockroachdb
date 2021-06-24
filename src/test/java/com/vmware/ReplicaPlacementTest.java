package com.vmware;

import com.vmware.generated.Tables;
import com.vmware.generated.tables.records.ReplicaRecord;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.TableField;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Unit test for simple App.
 */
public class ReplicaPlacementTest {
    @Test
    public void testConstraintTables() {
        final ReplicaPlacement placement = new ReplicaPlacement(Policies.defaultPolicies());

        placement.addNodeWithAttributes(1, List.of("region=east", "az=us-east-1"), List.of("ram:64GB"),
                List.of("ssd"));
        placement.addNodeWithAttributes(2, List.of("region=east", "az=us-east-1"), List.of("ram:64GB"),
                List.of("ssd"));
        placement.addNodeWithAttributes(3, List.of("region=east", "az=us-east-1"), List.of("ram:64GB"),
                List.of("ssd"));
        placement.addNodeWithAttributes(4, List.of("region=west", "az=us-west-1"), List.of("ram:64GB"),
                List.of("ssd"));
        placement.addNodeWithAttributes(5, List.of("region=west", "az=us-west-1"), List.of("ram:64GB"),
                List.of("ssd"));
        placement.addNodeWithAttributes(6, List.of("region=west", "az=us-west-1"), List.of("ram:64GB"),
                List.of("ssd"));

        placement.addDatabase("db1", 3, "[\"+ssd\", \"-region=east\"]");
        placement.addDatabase("db2", 3, "{'[\"+ssd\",\"+region=west\"]': 2, '[\"+region=east\"]': 1}");
        final Result<? extends Record> results = placement.placeReplicas();
        final TableField<ReplicaRecord, Integer> controllableNodeColumn = Tables.REPLICA.CONTROLLABLE__NODE;
        results.intoGroups(Tables.REPLICA.RANGE_ID).forEach(
                (rangeId, table) -> {
                    if (rangeId == 1) {
                        assertTrue(Set.of(4, 5, 6).containsAll(table.getValues(controllableNodeColumn)));
                    } else {
                        table.forEach(
                                record -> {
                                    if (record.get(Tables.REPLICA.ID) == 4 || record.get(Tables.REPLICA.ID) == 5) {
                                        assertTrue(Set.of(4, 5, 6).contains(record.getValue(controllableNodeColumn)));
                                    } else {
                                        assertTrue(Set.of(1, 2, 3).contains(record.getValue(controllableNodeColumn)));
                                    }
                                }
                        );
                    }
                }
        );
    }

    /*
     * Scenario 1:
     * By default, spread out replicas across AZs
     * https://www.cockroachlabs.com/docs/v21.1/configure-replication-zones#even-replication-across-availability-zones
     */
    @Test
    public void evenReplicationAcrossAZs() {
        final ReplicaPlacement placement = new ReplicaPlacement(Policies.defaultPolicies());
        // AZ-1
        placement.addNodeWithAttributes(1, List.of("az=us-1"), Collections.emptyList(), Collections.emptyList());
        placement.addNodeWithAttributes(2, List.of("az=us-1"), Collections.emptyList(), Collections.emptyList());

        // AZ-2
        placement.addNodeWithAttributes(3, List.of("az=us-2"), Collections.emptyList(), Collections.emptyList());
        placement.addNodeWithAttributes(4, List.of("az=us-2"), Collections.emptyList(), Collections.emptyList());

        // AZ-3
        placement.addNodeWithAttributes(5, List.of("az=us-3"), Collections.emptyList(), Collections.emptyList());
        placement.addNodeWithAttributes(6, List.of("az=us-3"), Collections.emptyList(), Collections.emptyList());

        // By default, all shards get 3 replicas each
        placement.addDatabase("db1");
        placement.addDatabase("db2");

        placement.printState();
        Result<? extends Record> results = placement.placeReplicas();

        // All nodes must be used
        assertEquals(6, results.stream().map(e -> e.get("CONTROLLABLE__NODE")).collect(Collectors.toSet()).size());

        // Each shard should have a different AZ
        for (final var shardId : results.intoGroups(Tables.REPLICA.RANGE_ID).entrySet()) {
            assertEquals(3, shardId.getValue().stream().map(e -> e.get("CONTROLLABLE__NODE"))
                    .collect(Collectors.toSet()).size());
        }
    }

    /*
     * Scenario 2:
     * https://www.cockroachlabs.com/docs/v21.1/configure-replication-zones\
     * #per-replica-constraints-to-specific-availability-zones
     */
    @Test
    public void perReplicaConstraintsToSpecificAvailabilityZones() {
        final ReplicaPlacement placement = new ReplicaPlacement(Policies.defaultPolicies());
        // West-1, AZ-a/b
        placement.addNodeWithAttributes(1, List.of("region=us-west1", "az=us-west1-a"),
                Collections.emptyList(), Collections.emptyList());
        placement.addNodeWithAttributes(2, List.of("region=us-west1", "az=us-west1-b"),
                Collections.emptyList(), Collections.emptyList());

        // Central-1, AZ-a
        placement.addNodeWithAttributes(3, List.of("region=us-central1", "az=us-central1-a"),
                Collections.emptyList(), Collections.emptyList());

        // East-1, AZ-a/b
        placement.addNodeWithAttributes(5, List.of("region=us-east1", "az=us-east1-a"),
                Collections.emptyList(), Collections.emptyList());
        placement.addNodeWithAttributes(6, List.of("region=us-east1", "az=us-east1-b"),
                Collections.emptyList(), Collections.emptyList());

        placement.addDatabase("db1"); // should be spread across all zones
        placement.addDatabase("west_app_db", 3,
                "{'[\"+region=us-west1\"]': 2, '[\"+region=us-central1\"]': 1}");

        placement.printState();
        Result<? extends Record> results = placement.placeReplicas();
        results.intoGroups(Tables.REPLICA.RANGE_ID).forEach((rangeId, table) -> {
            if (rangeId.equals(2)) { // west_app_db
                table.forEach(r -> {
                            final int id = r.get(Tables.REPLICA.ID);
                            final int node = r.get(Tables.REPLICA.CONTROLLABLE__NODE);
                            if (id == 4 || id == 5) {
                                assertTrue(List.of(1, 2).contains(node));
                            } else {
                                assertEquals(3, node);
                            }
                        }
                );
            }
        });
    }

    /*
     * Scenario 3:
     * https://www.cockroachlabs.com/docs/v21.1/configure-replication-zones
     * #multiple-applications-writing-to-different-databases
     */
    @Test
    public void multipleApplicationsWritingToDifferentDatabases() {
        final ReplicaPlacement placement = new ReplicaPlacement(Policies.defaultPolicies());

        // US-1
        placement.addNodeWithAttributes(1, List.of("az=us-1"),
                Collections.emptyList(), Collections.emptyList());
        placement.addNodeWithAttributes(2, List.of("az=us-1"),
                Collections.emptyList(), Collections.emptyList());
        placement.addNodeWithAttributes(3, List.of("az=us-1"),
                Collections.emptyList(), Collections.emptyList());

        // US-2
        placement.addNodeWithAttributes(4, List.of("az=us-2"),
                Collections.emptyList(), Collections.emptyList());
        placement.addNodeWithAttributes(5, List.of("az=us-2"),
                Collections.emptyList(), Collections.emptyList());
        placement.addNodeWithAttributes(6, List.of("az=us-2"),
                Collections.emptyList(), Collections.emptyList());


        placement.addDatabase("app1_db", 5, ""); // should be spread across all zones
        placement.addDatabase("app2_db", 3, "[\"+az=us-2\"]"); // should be spread across all zones

        Result<? extends Record> results = placement.placeReplicas();
        results.stream().filter(e -> e.get(Tables.REPLICA.RANGE_ID) == 2)
                .forEach(
                        r -> assertTrue(List.of(4, 5, 6).contains(r.get(Tables.REPLICA.CONTROLLABLE__NODE)))
                );
    }

    /*
     * Scenario 4:
     * https://www.cockroachlabs.com/docs/v21.1/configure-replication-zones
     * #stricter-replication-for-a-table-and-its-secondary-indexes
     */
    @Test
    public void stricterReplicationForATableAndItsSecondaryIndexes() {
        final ReplicaPlacement placement = new ReplicaPlacement(Policies.defaultPolicies());
        placement.addNodeWithAttributes(1, Collections.emptyList(),
                Collections.emptyList(), List.of("ssd"));
        placement.addNodeWithAttributes(2, Collections.emptyList(),
                Collections.emptyList(), List.of("ssd"));
        placement.addNodeWithAttributes(3, Collections.emptyList(),
                Collections.emptyList(), List.of("ssd"));
        placement.addNodeWithAttributes(4, Collections.emptyList(),
                Collections.emptyList(), List.of("ssd"));
        placement.addNodeWithAttributes(5, Collections.emptyList(),
                Collections.emptyList(), List.of("ssd"));
        placement.addNodeWithAttributes(6, Collections.emptyList(),
                Collections.emptyList(), List.of("hdd"));
        placement.addNodeWithAttributes(7, Collections.emptyList(),
                Collections.emptyList(), List.of("hdd"));

        // TODO: The actual example assigns constraints to only one table
        //       within this database. Update the schema to be able to do so.
        placement.addDatabase("db", 5, "[\"+ssd\"]"); // should be spread across all zones
        Result<? extends Record> results = placement.placeReplicas();
        results.stream().filter(e -> e.get(Tables.REPLICA.RANGE_ID) == 1)
                .forEach(
                        r -> assertTrue(List.of(1, 2, 3, 4, 5).contains(r.get(Tables.REPLICA.CONTROLLABLE__NODE)))
                );
    }

    /*
     * Scenario 5:
     * https://www.cockroachlabs.com/docs/v21.1/configure-replication-zones
     * #tweaking-the-replication-of-system-ranges
     */
    @Test
    public void tweakingTheReplicationOfSystemRanges() {
        final ReplicaPlacement placement = new ReplicaPlacement(Policies.defaultPolicies());
        for (int i = 1; i <= 7; i++) {
            placement.addNodeWithAttributes(i, List.of("az=us-" + i),
                    Collections.emptyList(), Collections.emptyList());
        }
        // TODO: have a default range and a metadata range
        placement.addDatabase("something", 5, ""); // should be spread across all zones
        placement.addDatabase("meta", 7, ""); // should be spread across all zones
        Result<? extends Record> results = placement.placeReplicas();
        assertEquals(7, results.stream().filter(e -> e.get(Tables.REPLICA.RANGE_ID) == 2)
                                        .collect(Collectors.toSet()).size());
    }


    /*
     * Performing replica placement a few at a time should not perturb existing allocations
     */
    @Test
    public void incrementalPlacement() {
        final ReplicaPlacement placement = new ReplicaPlacement(Policies.defaultPolicies());
        for (int i = 1; i <= 7; i++) {
            placement.addNodeWithAttributes(i, List.of("az=us-" + i),
                    Collections.emptyList(), Collections.emptyList());
        }
        // Place DB1 and record allocations
        placement.addDatabase("db1", 5, ""); // should be spread across all zones
        placement.placeReplicas();
        final Result<ReplicaRecord> replicaStateAfterDb1 = placement.getReplicaState();

        // Place DB2 and record allocations
        placement.addDatabase("db2", 5, ""); // should be spread across all zones
        placement.placeReplicas();
        final Result<ReplicaRecord> replicaStateAfterDb2 = placement.getReplicaState();
        final Map<?, Result<ReplicaRecord>> resultMapDb1 = replicaStateAfterDb1.intoGroups(Tables.REPLICA.RANGE_ID);
        final Map<?, Result<ReplicaRecord>> resultMapDb2 = replicaStateAfterDb2.intoGroups(Tables.REPLICA.RANGE_ID);

        // The allocations for db1 should not have changed
        assertEquals(resultMapDb1.get(1), resultMapDb2.get(1));
    }
}
