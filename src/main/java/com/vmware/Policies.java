/*
 * Copyright © 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware;

import java.util.ArrayList;
import java.util.List;

public class Policies {
    /*
     * Given that pending_replicas is a view, not a table, we explicitly configure a domain constraint
     */
    private static List<String> nodeDomain() {
        return List.of("CREATE VIEW node_domain AS " +
                "SELECT * FROM pending_replicas r " +
                "CHECK controllable__node IN (SELECT node.id FROM node)");
    }

    /*
     * Enforce the affinity/anti-affinities computed in the replica_to_node_constraint_matching view
     */
    private static List<String> nodeAffinityAndAntiAffinity() {
        return List.of("CREATE VIEW node_affinity_anti_affinity AS " +
                "SELECT * FROM pending_replicas r " +
                "JOIN replica_to_node_constraint_matching rncm ON " +
                " r.id = rncm.replica_id " +
                "CHECK (type != 'required' OR contains(rncm.node_id_list, r.controllable__node) = true)" +
                "AND (type != 'prohibited' OR contains(rncm.node_id_list, r.controllable__node) = false)");
    }

    /*
     * Spread out replicas across regions
     */
    private static List<String> spreadReplicasAcrossRegions() {
        final String countsPerShardPerRegionVariable = "CREATE VIEW count_per_shard_per_region AS " +
                "SELECT r.range_id, nr.region, count(*) as total " +
                "FROM pending_replicas r " +
                "JOIN node_regions nr" +
                " ON r.controllable__node = nr.node_id " +
                "GROUP BY r.range_id, nr.region";
        final String spreadReplicasAcrossRegions = "CREATE VIEW spread_replicas_regions AS " +
                "SELECT * FROM count_per_shard_per_region " +
                "GROUP BY range_id " +
                "MAXIMIZE min(total)";
        return List.of(countsPerShardPerRegionVariable, spreadReplicasAcrossRegions);
    }

    /*
     * Spread out replicas across availability zones
     */
    private static List<String> spreadReplicasAcrossAzs() {
        final String countsPerShardPerAzVariable = "CREATE VIEW count_per_shard_per_az AS " +
                                       "SELECT r.range_id, na.az, count(*) as total " +
                                       "FROM pending_replicas r " +
                                       "JOIN node_azs na" +
                                        " ON r.controllable__node = na.node_id " +
                                       "GROUP BY r.range_id, na.az";
        final String spreadReplicasAcrossAzs = "CREATE VIEW spread_replicas_azs AS " +
                                      "SELECT * FROM count_per_shard_per_az " +
                                      "GROUP BY range_id " +
                                      "MAXIMIZE min(total)";
        return List.of(countsPerShardPerAzVariable, spreadReplicasAcrossAzs);
    }

    /*
     * Use more nodes if possible
     */
    private static List<String> useMoreNodes() {
        final String countsPerShardPerAzVariable = "CREATE VIEW count_per_node AS " +
                                                   "SELECT count(*) as total " +
                                                   "FROM pending_replicas r " +
                                                   "JOIN node n" +
                                                   " ON r.controllable__node = n.id " +
                                                   "GROUP BY n.id";
        final String spreadReplicasAcrossNodes = "CREATE VIEW spread_replicas_across_nodes AS " +
                                                 "SELECT * FROM count_per_node " +
                                                 "MAXIMIZE min(total)";
        return List.of(countsPerShardPerAzVariable, spreadReplicasAcrossNodes);
    }

    /*
     * Replicas that already have assignments should not be moved
     */
    private static List<String> doNotReassignReplicas() {
        final String doNotReassignReplicas = "CREATE VIEW do_not_reassign AS " +
                                            "SELECT * " +
                                            "FROM pending_replicas " +
                                            "WHERE status = 'running' " +
                                            "CHECK current_node = controllable__node";
        return List.of(doNotReassignReplicas);
    }


    /*
     * Never assign two replicas to the same node
     */
    private static List<String> distributeAcrossDistinctNodes() {
        final String alwaysDistributeReplicasAcrossNodes = "CREATE VIEW distribute_across_nodes AS " +
                                                           "SELECT * " +
                                                           "FROM pending_replicas " +
                                                           "GROUP BY pending_replicas.range_id " +
                                                           "CHECK all_different(controllable__node)";
        return List.of(alwaysDistributeReplicasAcrossNodes);
    }

    public static List<String> defaultPolicies() {
        final List<String> policies = new ArrayList<>();
        policies.addAll(nodeDomain());
        policies.addAll(nodeAffinityAndAntiAffinity());
        policies.addAll(spreadReplicasAcrossRegions());
        policies.addAll(spreadReplicasAcrossAzs());
        policies.addAll(useMoreNodes());
        policies.addAll(distributeAcrossDistinctNodes());
        policies.addAll(doNotReassignReplicas());
        return policies;
    }
}
