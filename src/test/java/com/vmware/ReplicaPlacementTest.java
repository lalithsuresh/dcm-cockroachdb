package com.vmware;

import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Unit test for simple App.
 */
public class ReplicaPlacementTest {
    @Test
    public void quickStart() {
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

        placement.addReplica(1, 1, List.of("+ssd", "-region=east"));
        placement.addReplica(2, 1, List.of("+ssd", "-region=west"));
        placement.addReplica(3, 1, List.of("+ssd", "-region=west"));

        placement.printState();

        System.out.println("Input table");
        System.out.println(placement.getReplicaState());

        System.out.println("Output table");
        System.out.println(placement.placeReplicas());
    }
}
