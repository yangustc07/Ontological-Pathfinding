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

If you use Ontological Pathfinding in your research, please cite our paper:
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

 * Scala >= 2.11.7
 * sbt >= 0.13.7
 * Spark >= 1.5.1

Quick Start
-----------
To build the project, run:
```
$ sbt assembly
```

To mine inference rules (replace the Spark home, data paths, and
input parameters with your own configuration):
```
$ YOUR_SPARK_HOME/bin/spark-submit \
--class "Main" \
--master local[64] \
--driver-memory 400G \
--executor-memory 100G \
target/scala-2.11/Ontological-Pathfinding-assembly-1.0.jar \
learn --input-facts /path/to/data/facts.csv \
--input-rules /path/to/data/rules.csv \
--output-dir /path/to/output/ \
--rule-type 1 --functional-constraint 100 \
> /path/to/logs/prune.log
```

To infer missing facts:
```
$ YOUR_SPARK_HOME/bin/spark-submit \
--class "Main" \
--master local[64] \
--driver-memory 400G \
--executor-memory 100G \
target/scala-2.11/Ontological-Pathfinding-assembly-1.0.jar \
infer --input-facts /path/to/data/facts.csv \
--input-rules /path/to/data/rules.csv \
--output-dir /path/to/output/ \
--rule-type 1 --functional-constraint 100 \
> /path/to/logs/prune.log
```

To partition the knowledge base:
```
$ YOUR_SPARK_HOME/bin/spark-submit \
--class "Main" \
--master local[64] \
--driver-memory 400G \
--executor-memory 100G \
target/scala-2.11/Ontological-Pathfinding-assembly-1.0.jar \
partition --input-facts /path/to/data/facts.csv \
--input-rules /path/to/data/rules.csv \
--output-dir /path/to/output/ \
--max-facts 20000000 --max-rules 1000 \
```

The partitioning algorithm outputs the parts in `/path/to/output/part-i`. Thus,
it is helpful to use the following script to run the mining or inference
algorithm over all partitions:
```
#!/bin/bash

NUM_PARTS=$(ls -l /path/to/partition-output | grep 'part-' | wc -l)
for i in $(seq 0 $((NUM_PARTS-1))); do
  YOUR_SPARK_HOME/bin/spark-submit \
  --class "Main" \
  --master local[64] \
  --driver-memory 400G \
  --executor-memory 100G \
  target/scala-2.11/Ontological-Pathfinding-assembly-1.0.jar \
  learn --input-facts /path/to/partition-output/part-$i/facts.csv/'*' \
  --input-rules /path/to/partition-output/part-$i/rules.csv \
  --output-dir /path/to/mining-output/part-$i \
  --rule-type 1 --functional-constraint 100 \
  >>${LOG_DIR}/op.log
done

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
 9. p(x, y) <- q1(x, z1), q2(z2, z3), q3(z3, z4), q4(z4, z5), q5(z5, y)

Data
----
 * [Freebase data dump](https://developers.google.com/freebase/data); please
   contact [Yang Chen](mailto:yang@cise.ufl.edu) for the clean 388M Freebase facts.
 * Freebase schema
 * [36,625 Freebase first-order rules](http://cise.ufl.edu/~yang/data/Freebase-Rules.zip)

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
