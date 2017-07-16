\set schema_file /path/to/Ontological-Pathfinding/data/YAGOData/YAGOSchema.csv
\set rules_file  /path/to/Ontological-Pathfinding/data/YAGOData/YAGORules.csv

\set rules_file_1 :rules_file '-1'
\set rules_file_2 :rules_file '-2'
\set rules_file_3 :rules_file '-3'
\set rules_file_4 :rules_file '-4'
\set rules_file_5 :rules_file '-5'
\set rules_file_6 :rules_file '-6'

CREATE SCHEMA    op;
CREATE TABLE     op.Kbschema(predicate TEXT, domain TEXT, range TEXT);

COPY op.Kbschema FROM :'schema_file' DELIMITER ' ' QUOTE E'\b' CSV;

-- Type I
-- p(x, y) <- q(x, y)
COPY (SELECT DISTINCT h.predicate AS h, b.predicate AS b
      FROM   op.Kbschema h
      JOIN   op.Kbschema b
      ON     h.domain = b.domain AND h.range = b.range
      WHERE  h.predicate <> b.predicate)
TO :'rules_file_1' DELIMITER ' ';

-- Type II
-- p(x, y) <- q(y, x)
COPY (SELECT DISTINCT h.predicate AS h, b.predicate AS b
      FROM   op.Kbschema h
      JOIN   op.Kbschema b
      ON     h.domain = b.range AND h.range = b.domain)
TO :'rules_file_2' DELIMITER ' ';

-- Type III
-- p(x, y) <- q(z, x), r(z, y)
COPY (SELECT DISTINCT h.predicate AS h, b1.predicate AS b1, b2.predicate AS b2
      FROM   op.Kbschema h
      JOIN   op.Kbschema b1 ON h.domain = b1.range
      JOIN   op.Kbschema b2 ON h.range = b2.range
      WHERE  b1.domain = b2.domain)
TO :'rules_file_3' DELIMITER ' ';

-- Type IV
-- p(x, y) <- q(x, z), r(z, y)
COPY (SELECT DISTINCT h.predicate AS h, b1.predicate AS b1, b2.predicate AS b2
      FROM   op.Kbschema h
      JOIN   op.Kbschema b1 ON h.domain = b1.domain
      JOIN   op.Kbschema b2 ON h.range = b2.range
      WHERE  b1.range = b2.domain)
TO :'rules_file_4' DELIMITER ' ';

-- Type V
-- p(x, y) <- q(z, x), r(y, z)
COPY (SELECT DISTINCT h.predicate AS h, b1.predicate AS b1, b2.predicate AS b2
      FROM   op.Kbschema h
      JOIN   op.Kbschema b1 ON h.domain = b1.range
      JOIN   op.Kbschema b2 ON h.range = b2.domain
      WHERE  b1.domain = b2.range)
TO :'rules_file_5' DELIMITER ' ';

-- Type VI
-- p(x, y) <- q(x, z), r(y, z)
COPY (SELECT DISTINCT h.predicate AS h, b1.predicate AS b1, b2.predicate AS b2
      FROM   op.Kbschema h
      JOIN   op.Kbschema b1 ON h.domain = b1.domain
      JOIN   op.Kbschema b2 ON h.range = b2.domain
      WHERE  b1.range = b2.range)
TO :'rules_file_6' DELIMITER ' ';

DROP SCHEMA      op CASCADE;
