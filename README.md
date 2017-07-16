Ontological Pathfinding
=======================

Ontological Pathfinding (OP) is a scalable first-order rule mining algorithm.
It achieves scalability via a series of parallelization and optimization
techniques: a relational knowledge base model to apply inference rules in
batches, a new rule mining algorithm that parallelizes the join queries, a
novel partitioning algorithm to break the mining tasks into smaller independent
sub-tasks, and a pruning strategy to eliminate unsound and resource-consuming
rules before applying them. Combining these techniques, OP is the first rule
mining algorithm that mines 36,625 inference rules from Freebase, the largest
public knowledge base with 112 million entities and 388 million facts.

License 
-------
This repository is released under the
[BSD license](http://www.freebsd.org/copyright/freebsd-license.html).

If you use Ontological Pathfinding in your research, please cite our paper,
[Ontological Pathfinding: Mining First-Order Knowledge from Large Knowledge
Bases](http://www.cise.ufl.edu/~yang/doc/sigmod16/paper.pdf) from [ACM SIGMOD
2016](http://sigmod2016.org):
```
@inproceedings{chen2016ontological,
  title={Ontological Pathfinding: Mining First-Order Knowledge from Large Knowledge Bases},
  author={Chen, Yang and Goldberg, Sean and Wang, Daisy Zhe and Johri, Soumitra Siddharth},
  booktitle={Proceedings of the 2016 ACM SIGMOD international conference on Management of data},
  year={2016},
  organization={ACM}
}
```

Prerequisites
-------------

 * Scala 2.10.4
 * sbt >= 0.13.7
 * Spark >= 1.5.1
 * PostgreSQL >= 9.2.3

Quick Start
-----------
To build the project, run:
```
~/op$ sbt assembly
```

Extract the dataset:
```
~/op$ cd data/YAGOData
~/op/data/YAGOData$ gzip -d YAGOFacts.csv.gz
~/op/data/YAGOData$ gzip -d YAGOSchema.csv.gz
~/op/data/YAGOData$ unzip YAGORules.zip
```

We use PostgreSQL to construct candidate rules. To correctly set up the
database, you need a user, e.g., "op":

```
$ createuser -s -P op
```

and a database, e.g., "op":
```
$ createdb op
```

Variables to set in `run.sh`:
```
SPARK_PATH=${HOME}/spark-1.5.1/bin/spark-submit
JAR_PATH=target/scala-2.10/Ontological-Pathfinding-assembly-1.0.jar
MAIN_CLASS=Main
NCORES=64
DRIVER_MEMORY=400G
EXECUTOR_MEMORY=100G

POSTGRESQL_BIN=  # default to /usr/local/pgsql/bin
POSTGRESQL_HOST=localhost
POSTGRESQL_USER=op
POSTGRESQL_DB=op
```

Run the script:
```
~/op$ ./run.sh
16:55:33 [INFO] Ontological Pathfinding
16:55:33 [INFO] 1. Rule mining.
16:55:33 [INFO] 2. Knowledge expansion.
Choice: [1/2/q]1
16:55:34 [INFO] Mapping facts file "data/YAGOData/YAGOFacts.csv" to integer representation.
16:56:47 [INFO] Mapping rules file "data/YAGOData/YAGORules.csv-1" to integer representation.
16:56:47 [INFO] Partitioning KB "data/YAGOData/YAGOFacts.csv" and "data/YAGOData/YAGORules.csv-1" into subsets with max facts = 2000000 and max rules = 1000.
16:57:02 [INFO] Mining rules from "data/YAGOData/YAGOFacts.csv" and "data/YAGOData/YAGORules.csv-1."
16:57:02 [INFO] Mining partition 1/1.
16:57:21 [INFO] Writing output rules to "output-rules/1."
...
17:17:34 [INFO] Rule mining finishes.
```

If a KB schema schema is not provided, OP will try to generate a "universal
schema"
```
predicate_i universal universal
```

for every predicate.

Finally, view the result:
```
~/op$ less output-rules/[1-6]/rules
```

Supported Rule Types
--------------------
To use one of the following rule types, specify it as the `--rule-type` argument.

 1. p(x, y) <- q(x, y)
 2. p(x, y) <- q(y, x)
 3. p(x, y) <- q(z, x), r(z, y)
 4. p(x, y) <- q(x, z), r(z, y)
 5. p(x, y) <- q(z, x), r(y, z)
 6. p(x, y) <- q(x, z), r(y, z)
 7. p(x, y) <- q1(x, z1), q2(z2, z3), q3(z3, y)
 8. p(x, y) <- q1(x, z1), q2(z2, z3), q3(z3, z4), q4(z4, y)

To add a new rule type, you need to

 1. Add the data file to the data directory as defined in run.sh.
 2. Implement the rule construction query in op.sql.
 3. Implement the rule mining algorithm in src/main/scala/OPLearners.scala.
 4. Update INPUT_RULES_TYPES in run.sh.

Data
----
 * [36,625 Freebase first-order rules](https://docs.google.com/spreadsheets/d/1iyQdTnz5kGHYlyObVAMlZc3I7H5Jkqzuyie4o5wD-Jo/edit?usp=sharing)
 * [YAGO rules](https://docs.google.com/spreadsheets/d/10VFneJlHLxyR4WgjRO75Z3RstvApaR6fm76e1haSpEc/edit?usp=sharing)
 * Clean Freebase 388M facts: please contact [Yang Chen](mailto:yang@cise.ufl.edu).
 * [Clean YAGO2s data](data/YAGOData); You can also find the original [AMIE](https://www.mpi-inf.mpg.de/departments/databases-and-information-systems/research/yago-naga/amie)
   and [YAGO](http://www.mpi-inf.mpg.de/departments/databases-and-information-systems/research/yago-naga/yago/downloads)
   data on their websites.

Acknowledgments
---------------
The [ProbKB
project](http://dsr.cise.ufl.edu/projects/probkb-web-scale-probabilistic-knowledge-base)
is partially supported by NSF IIS Award # 1526753, DARPA under
FA8750-12-2-0348-2 (DEFT/CUBISM), and a generous gift from Google. We also
thank [Dr. Milenko Petrovic](http://www.ihmc.us/groups/mpetrovic) and [Dr. Alin
Dobra](http://www.cise.ufl.edu/~adobra) for the helpful discussions on query
optimization.

Contact
-------
If you have any questions about Ontological Pathfinding, please visit the
[project
website](http://dsr.cise.ufl.edu/projects/probkb-web-scale-probabilistic-knowledge-base)
or contact [Yang Chen](http://www.cise.ufl.edu/~yang), [Dr. Daisy Zhe
Wang](http://www.cise.ufl.edu/~daisyw), [DSR Lab @
UF](http://dsr.cise.ufl.edu).
