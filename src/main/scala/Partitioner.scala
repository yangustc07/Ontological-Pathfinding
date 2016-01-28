import scala.collection.Map
import scala.collection.immutable.Set
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Builder

// This class represents a KB partition.
class Partition (
  // Each partition tracks the predicates, rules, and its size (i.e.,
  // number of facts).
  val predicates: Set[Int],
  val rules: Array[Array[Int]],
  val size: Int
)

// A partition builder accepts new rules to be added. As inference rules are
// added to the partition, this builder maintains the set of predicates, rules,
// and the KB size. These data allow clients to efficiently compute the
// partition size increased by adding a new rule to this partition.
class PartitionBuilder(val histogram: Map[Int, Int])
  extends Builder[Array[Int], Partition] {
  // Adds a new rule to this partition. Each rule is represented as an array of
  // predicate in strings (head, body1, body2, ..., bodyn). The predicates set,
  // rules set, KB size are updated accordingly.
  override def += (rule: Array[Int]): PartitionBuilder.this.type = {
    rule.tail.foreach(pred => { numFacts += (
      if (predicates.add(pred)) histogram.getOrElse(pred, Int.MinValue)
      else 0)})
    rules += rule
    this
  }

  // Clears this partition.
  override def clear() {
    predicates.clear
    rules.clear
    numFacts = 0
  }

  // Returns the result (immutable Partition) from this partition builder.
  override def result(): Partition = {
    new Partition(predicates.toSet, rules.toArray, size)
  }

  // Returns the result (immutable Partition) from this partition builder.
  def toPartition = result

  // Returns the size, i.e., number of facts, in this partition.
  def size = numFacts

  // Returns true if the partition is empty.
  def isEmpty = rules.isEmpty

  // Returns the rules in this partition.
  def getRules = rules.clone

  // Computes the number of additional facts to add to this partition if we add
  // new 'rule' to it.
  def sizeIncreaseByRule(rule: Array[Int]) = {
    numFacts/50.0 + rule.tail.foldLeft(0)((sum, pred) => sum + (
        if (predicates.contains(pred)) 0
        else histogram.getOrElse(pred, Int.MinValue)))
  }

  // Set of predicates in this partition.
  private val predicates = new scala.collection.mutable.HashSet[Int]

  // Set of rules in this partition, each represented as an array of predicates
  // of form (head, body1, body2, ..., bodyn).
  private val rules = new ArrayBuffer[Array[Int]]

  // Number of facts in this partition, belonging to the predicates in
  // 'predicates'.
  private var numFacts = 0
}

// The Partitioner class does the actual partitioning. A Partitioner receives
// a set of rules to be partitioned, a histogram that records the number of
// facts of each predicate, the maximum partition size and the maximum rule
// numbers of each partition.
class Partitioner(rules: Array[Array[Int]], histogram: Map[Int, Int],
                  maxFacts: Int, maxRules: Int) {
  // The 'partitions' receives new partitions that satisfies the size contraints
  // as the algorithm proceeds.
  private val partitions = new ArrayBuffer[Partition]

  // The 'initialPartitions' contains all the rules.
  private val initialPartitions = rules
    .filter(rule => rule.tail.forall(histogram.contains(_)))
    .foldLeft(new PartitionBuilder(histogram))((part, r) => part += r)

  recPartition(initialPartitions)

  // Returns the partitions as an immutable list.
  def getPartitions = partitions.toArray

  // Recursively partitions the input partition until all partitions satisfy the
  // size constraints. The partitions are recorded in variable 'partitions'.
  private def recPartition(partition: PartitionBuilder): Unit = {
    if (partition.size <= maxFacts &&
        partition.getRules.size <= maxRules) {
      partitions += partition.toPartition
      return
    }
    val (part1, part2) = biPartition(partition)
    if (part1.isEmpty) partitions += part2.toPartition
    else if (part2.isEmpty) partitions += part1.toPartition
    else {
      recPartition(part1)
      recPartition(part2)
    }
  }

  // Partition the input 'partition' into two subpartitions. Applies a greedy
  // strategy to get even partitions.
  private def biPartition(partition: PartitionBuilder) = {
    val part1 = new PartitionBuilder(histogram)
    val part2 = new PartitionBuilder(histogram)
    var rules = partition.getRules
    for (r <- rules) {
      if (part1.sizeIncreaseByRule(r) < part2.sizeIncreaseByRule(r))
        part1 += r
      else
        part2 += r
    }
    (part1, part2)
  }
}
