CREATE TABLE node (
    id INTEGER NOT NULL PRIMARY KEY
);

CREATE TABLE node_label (
    id INTEGER NOT NULL,
    label_key VARCHAR(30) NOT NULL,
    label_value VARCHAR(30),
    FOREIGN KEY (id) REFERENCES node(id) ON DELETE CASCADE
);

CREATE TABLE database (
    id INTEGER NOT NULL PRIMARY KEY auto_increment,
    name VARCHAR(100) NOT NULL,
    num_replicas INTEGER NOT NULL
);

CREATE TABLE range (
    id INTEGER NOT NULL PRIMARY KEY auto_increment,
    database_id INTEGER NOT NULL,
    FOREIGN KEY (database_id) REFERENCES database(id) ON DELETE CASCADE
);

-- TODO: add non-voting/voting replica distinction
CREATE TABLE replica (
    id INTEGER NOT NULL PRIMARY KEY auto_increment,
    range_id INTEGER NOT NULL,
    status VARCHAR(10) NOT NULL,
    controllable__node INTEGER,
    FOREIGN KEY (controllable__node) REFERENCES node(id) ON DELETE CASCADE,
    FOREIGN KEY (range_id) REFERENCES range(id) ON DELETE CASCADE
);

CREATE TABLE replica_constraint (
    id INTEGER NOT NULL,
    range_id INTEGER NOT NULL,
    type VARCHAR(30) NOT NULL,
    label_key VARCHAR(30) NOT NULL,
    label_value VARCHAR(30),
    FOREIGN KEY (id) REFERENCES replica(id) ON DELETE CASCADE
);

-- For each replica, compute the set of nodes they are affine
-- or anti-affine to (depending on the type of replica constraint)
CREATE VIEW replica_to_node_constraint_matching AS
    SELECT rc.id AS replica_id,
           rc.range_id,
           rc.type,
           rc.label_key,
           ARRAY_AGG(nl.id) AS node_id_list
    FROM replica_constraint rc
    JOIN node_label nl
        ON   rc.label_key = nl.label_key
         AND (rc.label_value IS NULL
              OR rc.label_value = nl.label_value)
    GROUP BY rc.id, rc.range_id, rc.type, rc.label_key;


-- Find the AZ for each node, if configured
CREATE VIEW node_azs AS
    SELECT node.id AS node_id,
           nl.label_value AS az
    FROM node_label nl
    JOIN node
        ON node.id = nl.id
    WHERE nl.label_key = 'az' AND nl.label_value IS NOT NULL;