package com.vmware;

import com.google.common.collect.Sets;
import com.vmware.generated.Tables;
import com.vmware.generated.tables.records.ReplicaRecord;
import org.jooq.Result;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Unit tests for placement logic
 */
public class ReplicaPlacementTest {
    @Test
    public void testConstraintTables() {
        final ReplicaPlacement placement = ReplicaPlacement.init();

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
        placement.placeReplicas();
        final Result<ReplicaRecord> db1 = placement.getReplicaRangesForDb("db1");
        final Result<ReplicaRecord> db2 = placement.getReplicaRangesForDb("db2");
        final Set<Integer> db1Nodes = db1.intoSet(Tables.REPLICA.CURRENT_NODE);
        assertEquals(db1Nodes, Set.of(4, 5, 6));
        final List<Integer> db2Nodes = db2.getValues(Tables.REPLICA.CURRENT_NODE);
        assertTrue(Set.of(4, 5, 6).contains(db2Nodes.get(0)));
        assertTrue(Set.of(4, 5, 6).contains(db2Nodes.get(1)));
        assertTrue(Set.of(1, 2, 3).contains(db2Nodes.get(2)));
    }

    /*
     * Scenario 1:
     * By default, spread out replicas across AZs
     * https://www.cockroachlabs.com/docs/v21.1/configure-replication-zones#even-replication-across-availability-zones
     */
    @Test
    public void evenReplicationAcrossAZs() {
        final ReplicaPlacement placement = ReplicaPlacement.init();
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

        placement.placeReplicas();

        final Result<ReplicaRecord> db1 = placement.getReplicaRangesForDb("db1");
        final Result<ReplicaRecord> db2 = placement.getReplicaRangesForDb("db2");
        final Set<Integer> db1Nodes = db1.intoSet(Tables.REPLICA.CURRENT_NODE);
        final Set<Integer> db2Nodes = db2.intoSet(Tables.REPLICA.CURRENT_NODE);
        // All nodes must be used
        assertEquals(Set.of(1, 2, 3, 4, 5, 6), Sets.union(db1Nodes, db2Nodes));

        // Each shard should have a different AZ
        for (final var nodes: List.of(db1Nodes, db2Nodes)) {
            assertEquals(3, nodes.size());
            assertFalse(nodes.contains(1) && nodes.contains(2));
            assertFalse(nodes.contains(3) && nodes.contains(4));
            assertFalse(nodes.contains(5) && nodes.contains(6));
        }
    }

    /*
     * Scenario 2:
     * https://www.cockroachlabs.com/docs/v21.1/configure-replication-zones\
     * #per-replica-constraints-to-specific-availability-zones
     */
    @Test
    public void perReplicaConstraintsToSpecificAvailabilityZones() {
        final ReplicaPlacement placement = ReplicaPlacement.init();
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
        placement.bootstrap();
        placement.printState();
        placement.placeReplicas();

        final Result<ReplicaRecord> db1 = placement.getReplicaRangesForDb("db1");
        final Set<Integer> db1Nodes = db1.intoSet(Tables.REPLICA.CURRENT_NODE);
        placement.printState();
        assertTrue(db1Nodes.contains(1) || db1Nodes.contains(2));
        assertTrue(db1Nodes.contains(3));
        assertTrue(db1Nodes.contains(5) || db1Nodes.contains(6));

        final Result<ReplicaRecord> westAppDb = placement.getReplicaRangesForDb("west_app_db");
        final List<Integer> westAppDbNodes = westAppDb.getValues(Tables.REPLICA.CURRENT_NODE);

        assertTrue(List.of(1, 2).containsAll(westAppDbNodes.subList(0, 2)));
        assertEquals(3, westAppDbNodes.get(2));
    }

    /*
     * Scenario 3:
     * https://www.cockroachlabs.com/docs/v21.1/configure-replication-zones
     * #multiple-applications-writing-to-different-databases
     */
    @Test
    public void multipleApplicationsWritingToDifferentDatabases() {
        final ReplicaPlacement placement = ReplicaPlacement.init();

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
        placement.bootstrap();

        placement.addDatabase("app1_db", 5, ""); // should be spread across all zones
        placement.addDatabase("app2_db", 3, "[\"+az=us-2\"]"); // should be confined to zone 2

        placement.placeReplicas();
        final Set<Integer> app1DbNodes = placement.getReplicaRangesForDb("app1_db")
                .intoSet(Tables.REPLICA.CURRENT_NODE);
        assertTrue(Sets.intersection(Set.of(1, 2, 3), app1DbNodes).size() > 1);
        assertTrue(Sets.intersection(Set.of(4, 5, 6), app1DbNodes).size() > 1);
        final Set<Integer> app2DbNodes = placement.getReplicaRangesForDb("app2_db")
                                             .intoSet(Tables.REPLICA.CURRENT_NODE);
        assertEquals(Set.of(4, 5, 6), app2DbNodes);
    }

    /*
     * Scenario 4:
     * https://www.cockroachlabs.com/docs/v21.1/configure-replication-zones
     * #stricter-replication-for-a-table-and-its-secondary-indexes
     */
    @Test
    public void stricterReplicationForATableAndItsSecondaryIndexes() {
        final ReplicaPlacement placement = ReplicaPlacement.init();
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
        placement.bootstrap();

        // TODO: The actual example assigns constraints to only one table
        //       within this database. Update the schema to be able to do so.
        placement.addDatabase("db", 5, "[\"+ssd\"]"); // should be spread across all zones
        placement.placeReplicas();
        final Set<Integer> dbNodes = placement.getReplicaRangesForDb("db").intoSet(Tables.REPLICA.CURRENT_NODE);
        assertEquals(Set.of(1, 2, 3, 4, 5), dbNodes);
    }

    /*
     * Scenario 5:
     * https://www.cockroachlabs.com/docs/v21.1/configure-replication-zones
     * #tweaking-the-replication-of-system-ranges
     */
    @Test
    public void tweakingTheReplicationOfSystemRanges() {
        final ReplicaPlacement placement = ReplicaPlacement.init();
        for (int i = 1; i <= 7; i++) {
            placement.addNodeWithAttributes(i, List.of("az=us-" + i),
                    Collections.emptyList(), Collections.emptyList());
        }
        placement.bootstrap();
        placement.editDatabase("meta", 7, ""); // should be spread across all zones
        placement.placeReplicas();
        final Set<Integer> dbNodes = placement.getReplicaRangesForDb("meta").intoSet(Tables.REPLICA.CURRENT_NODE);
        assertEquals(7, dbNodes.size());
    }


    /*
     * Performing replica placement a few at a time should not perturb existing allocations
     */
    @Test
    public void incrementalPlacement() {
        final ReplicaPlacement placement = ReplicaPlacement.init();
        for (int i = 1; i <= 7; i++) {
            placement.addNodeWithAttributes(i, List.of("az=us-" + i),
                    Collections.emptyList(), Collections.emptyList());
        }
        placement.bootstrap();
        // Place DB1 and record allocations
        placement.addDatabase("db1", 5, ""); // should be spread across all zones
        placement.placeReplicas();
        final Result<ReplicaRecord> replicaStateAfterDb1 = placement.getReplicaState();

        // Place DB2 and record allocations
        placement.addDatabase("db2", 5, ""); // should be spread across all zones
        placement.placeReplicas();
        final Result<ReplicaRecord> replicaStateAfterDb2 = placement.getReplicaState();
        final Map<Integer, Result<ReplicaRecord>> resultMapDb1 = replicaStateAfterDb1
                                                                    .intoGroups(Tables.REPLICA.RANGE_ID);
        final Map<Integer, Result<ReplicaRecord>> resultMapDb2 = replicaStateAfterDb2
                                                                    .intoGroups(Tables.REPLICA.RANGE_ID);
        // The allocations for db1 should not have changed
        assertEquals(resultMapDb1.get(1), resultMapDb2.get(1));
    }
}
