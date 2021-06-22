CREATE TABLE node (
    id INTEGER NOT NULL
);

CREATE TABLE node_label (
    id INTEGER NOT NULL,
    label_key VARCHAR(30) NOT NULL,
    label_value VARCHAR(30),
    FOREIGN KEY (id) REFERENCES node(id) ON DELETE CASCADE
);

CREATE TABLE replica (
    id INTEGER NOT NULL,
    shardId INTEGER NOT NULL,
    status VARCHAR(10) NOT NULL,
    controllable__node INTEGER,
    FOREIGN KEY (controllable__node) REFERENCES node(id) ON DELETE CASCADE
);

CREATE TABLE replica_constraint (
    id INTEGER NOT NULL,
    shardId INTEGER NOT NULL,
    type VARCHAR(30) NOT NULL,
    label_key VARCHAR(30) NOT NULL,
    label_value VARCHAR(30),
    FOREIGN KEY (id) REFERENCES replica(id) ON DELETE CASCADE
);

CREATE VIEW replica_to_node_constraint_matching AS
    SELECT rc.id AS replica_id,
           rc.shardId,
           rc.type,
           ARRAY_AGG(nl.id) AS node_id
    FROM replica_constraint rc
    JOIN node_label nl
        ON   rc.label_key = nl.label_key
         AND (rc.label_value IS NULL
              OR rc.label_value = nl.label_value)
    GROUP BY rc.id, rc.shardId, rc.type;