import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import scala.collection.mutable.HashSet

@SerialVersionUID(171L)
abstract class OPLearnerBase extends Serializable {
  // Computes confidence scores for inference rules in 'rules'.
  //
  // 'facts' is an RDD of facts represented by arrays of form:
  //   [predicate ID, subject ID, object ID].
  // 'rules' is a broadcast variable of candidate rules of form:
  //   [rule ID, head ID, body1 ID, body2 ID].
  // Returns an RDD of inference rules. Each rule is represented by an array:
  //   [rule ID, support, confidence].
  def learn(facts: RDD[Array[Int]], rules: Broadcast[Array[Array[Int]]],
            constraint: Int) = {
    applyRules(facts, rules, constraint)
      .flatMap({case (fact, rules) => check(rules)})
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2), NUM_COUNT_TASKS)
      .map({case (rid, (sum, cnt)) => (rid, sum, sum.toDouble/cnt.toDouble)})
  }

  // Derives implicit facts from the input knowledge base 'facts' using 'rules'.
  //
  // Returns an RDD of inferred facts. Each fact is represented by a triple
  //   (predicate ID, subject ID, object ID)
  // and a list of rule IDs that infer this fact.
  //
  // The input parameters are same as 'learn'.
  def infer(facts: RDD[Array[Int]], rules: Broadcast[Array[Array[Int]]],
            constraint: Int) = {
    applyRules(facts, rules, constraint)
      .filter({case (fact, rules) => !rules.contains(BASE_FACT)})
  }

  // Applies inference rules. This step is needed by both rule learning and
  // inference to generate a set of inferred facts. Since the rule application
  // process is different among rule types, the method is abstract in the base
  // class, and the subclasses (Types 3-6) should provide an implementation
  // based on the rule type.
  //
  // Returns an RDD of facts, and IDs of rules that generate these facts. A
  // special ID, 'BASE_FACT', should be used to indicate that the fact is from
  // the original KB.
  //
  // The input parameters are same as 'learn' and 'infer'.
  protected def applyRules(facts: RDD[Array[Int]],
                           rules: Broadcast[Array[Array[Int]]],
                           constraint: Int):
      RDD[((Int, Int, Int), HashSet[Int])]

  // Checks if inference rules in 'rules' are performing a correct inference by
  // examining if the 'BASE_FACT' ID is present in the rules set.
  //
  // Returns an RDD of rule IDs, the correct inference count, and the total
  // inference count, of the form
  //   (rule ID, (correct count, total count)).
  private def check(rules: HashSet[Int]) = { 
    val correct = if (rules.contains(BASE_FACT)) 1 else 0
    for(r <- rules if r != BASE_FACT)
      yield (r, (correct /* sum */, 1 /* count */))
  }

  // Special ID used to indicate a fact coming from the original knowledge base.
  // This must not be a possible rule ID.
  protected val BASE_FACT = 0

  // Spark configurations.
  protected val NUM_INPUT_TASKS = 128
  protected val NUM_JOIN_TASKS = 512
  protected val NUM_CHECK_TASKS = 128
  protected val NUM_COUNT_TASKS = 128
}
