/*
 * Copyright © 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware;

import java.util.ArrayList;
import java.util.List;

public class Policies {
    /*
     * Enforce the affinity/anti-affinities computed in the replica_to_node_constraint_matching view
     */
    private static List<String> nodeAffinityAndAntiAffinity() {
        return List.of("CREATE VIEW node_affinity_anti_affinity AS " +
                "SELECT * FROM replica r " +
                "JOIN replica_to_node_constraint_matching rncm ON " +
                " r.id = rncm.replica_id " +
                "CHECK (type != 'Required' OR contains(rncm.node_id_list, r.controllable__node) = true)" +
                "  AND (type != 'Prohibited' OR contains(rncm.node_id_list, r.controllable__node) = false)");
    }

    /*
     * Spread out replicas across availability zones
     */
    private static List<String> spreadReplicasAcrossAzs() {
        final String countsPerShardPerAzVariable = "CREATE VIEW count_per_shard_per_az AS " +
                                       "SELECT r.range_id, na.az, count(*) as total " +
                                       "FROM replica r " +
                                       "JOIN node_azs na" +
                                        " ON r.controllable__node = na.node_id " +
                                       "GROUP BY r.range_id, na.az";
        final String spreadReplicasAcrossAzs = "CREATE VIEW spread_replicas AS " +
                                      "SELECT * FROM count_per_shard_per_az " +
                                      "GROUP BY range_id " +
                                      "maximize min(total)";
        return List.of(countsPerShardPerAzVariable, spreadReplicasAcrossAzs);
    }

    /*
     * Use more nodes if possible
     */
    private static List<String> useMoreNodes() {
        final String countsPerShardPerAzVariable = "CREATE VIEW count_per_node AS " +
                                                   "SELECT count(*) as total " +
                                                   "FROM replica r " +
                                                   "JOIN node n" +
                                                   " ON r.controllable__node = n.id " +
                                                   "GROUP BY n.id";
        final String spreadReplicasAcrossNodes = "CREATE VIEW spread_replicas_across_nodes AS " +
                                                 "SELECT * FROM count_per_node " +
                                                 "MAXIMIZE min(total)";
        return List.of(countsPerShardPerAzVariable, spreadReplicasAcrossNodes);
    }


    public static List<String> defaultPolicies() {
        final List<String> policies = new ArrayList<>();
        policies.addAll(nodeAffinityAndAntiAffinity());
        policies.addAll(spreadReplicasAcrossAzs());
        policies.addAll(useMoreNodes());
        return policies;
    }
}
