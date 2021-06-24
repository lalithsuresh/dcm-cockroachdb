package com.vmware;

import com.vmware.generated.Tables;
import com.vmware.generated.tables.records.ReplicaRecord;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.TableField;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
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
        for (final var shardId: results.intoGroups(Tables.REPLICA.RANGE_ID).entrySet()) {
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
}
