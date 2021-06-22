/*
 * Copyright © 2018-2021 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware;

import java.util.List;

public class Policies {
    /*
     * Enforce the affinity/anti-affinities computed in the replica_to_node_constraint_matching view
     */
    private static String nodeAffinityAndAntiAffinity() {
        return "CREATE VIEW node_affinity_anti_affinity AS " +
                "SELECT * FROM replica r " +
                "JOIN replica_to_node_constraint_matching rncm ON " +
                " r.id = rncm.replica_id " +
                "CHECK (type != 'Required' OR contains(rncm.node_id, r.controllable__node) = true)" +
                "  AND (type != 'Prohibited' OR contains(rncm.node_id, r.controllable__node) = false)";
    }

    public static List<String> defaultPolicies() {
        return List.of(nodeAffinityAndAntiAffinity());
    }
}
