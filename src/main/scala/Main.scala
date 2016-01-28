import java.io.File
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.HashSet
import scala.io.Source

case class Config(
  inputFacts: String = "", inputRules: String = "", outputDir: String = "",
  outputFacts: String = "facts.csv", outputRules: String = "rules.csv",
  task: String = "", maxFacts: Int = -1, maxRules: Int = -1, ruleType: Int = -1,
  functionalConstraint: Int = 100, trueFacts: String = ""
)

object Main {
  private val conf = new SparkConf().setAppName("Ontological Pathfinding")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  private val spark = new SparkContext(conf)
  private val logger = Logger.getLogger(getClass.getName)

  private val options = new scopt.OptionParser[Config]("op") {
    head("Ontological Pathfinding", "1.0")
    opt[String]("input-facts") required() action { (value, option) =>
      option.copy(inputFacts = value) } text("Path to input facts.")
    opt[String]("input-rules") required() action { (value, option) =>
      option.copy(inputRules = value) } text("Path to input rules.")
    opt[String]("output-dir") required() action { (value, option) =>
      option.copy(outputDir = value) } text("Path to output directory.")
    cmd("partition") action { (_, option) =>
      option.copy(task = "partition") } text(
      "Partition the input KB.") children(
        opt[Int]("max-facts") required() action { (value, option) =>
          option.copy(maxFacts = value) } text("Maximum facts."),
        opt[Int]("max-rules") required() action { (value, option) =>
          option.copy(maxRules = value) } text("Maximum rules."),
        opt[String]("output-facts") optional() action { (value, option) =>
          option.copy(outputFacts = value) } text("Output facts."),
        opt[String]("output-rules") optional() action { (value, option) =>
          option.copy(outputRules = value) } text("Output rules.")
      )
    cmd("learn") action { (_, option) =>
      option.copy(task = "learn") } text("Learn inference rules.") children(
        opt[Int]("rule-type") required() action { (value, option) =>
          option.copy(ruleType = value) } validate { x => if (1 <= x && x <= 9)
            success else failure("Supporting rule types 1-9.")
          } text("Rule type."),
        opt[Int]("functional-constraint") abbr("fc") action { (value, option) =>
          option.copy(functionalConstraint = value) }
          text("Functional constraint.")
    )
    cmd("infer") action { (_, option) =>
      option.copy(task = "infer") } text("Infer new facts.") children(
        opt[Int]("rule-type") required() action { (value, option) =>
          option.copy(ruleType = value) } validate { x => if (1 <= x && x <= 9)
            success else failure("Supporting rule types 1-9.")
          } text("Rule type."),
        opt[Int]("functional-constraint") abbr("fc") action { (value, option) =>
          option.copy(functionalConstraint = value) }
          text("Functional constraint.")
      )
    cmd("evaluate") action { (_, option) =>
      option.copy(task = "evaluate") } text("Infer and evaluate.") children(
        opt[String]("ground-truth") required() action { (value, option) =>
          option.copy(trueFacts = value) } text("Ground truth facts.")
      )
  }

  def main(args: Array[String]) {
    options.parse(args, Config()) match {
      case Some(config) => config.task match {
        case "partition" => partition(config)
        case "learn" => learn(config)
        case "infer" => infer(config)
        case "evaluate" => evaluate(config)
      }
      case None => logger.fatal("Abort.")
    }
  }

  // Runs the rule learning algorithm.
  private def learn(config: Config) {
    val facts = spark.textFile(config.inputFacts, 64)
      .map(fact => fact.split(" ").map(_.toInt))

    val rules = Source.fromFile(config.inputRules).getLines()
      .map(rule => rule.split(" ").map(_.toInt))
      .toArray

    OPLearners(config.ruleType)
      .learn(facts, spark.broadcast(rules), config.functionalConstraint)
      .filter({case (rule, supp, conf) => conf > 0})
      .map(_.productIterator.mkString(" "))
      .saveAsTextFile(config.outputDir)
  }
  
  private def infer(config: Config) {
    val facts = spark.textFile(config.inputFacts, 64)
      .map(fact => fact.split(" ").map(_.toInt))

    val rules = Source.fromFile(config.inputRules).getLines()
      .map(rule => rule.split(" ").map(_.toInt))
      .toArray

    OPLearners(config.ruleType)
      .infer(facts, spark.broadcast(rules), config.functionalConstraint)
      .map({case (fact, rules) =>
        fact.productIterator.mkString(" ") + ":" + rules.mkString(" ")})
      .saveAsTextFile(config.outputDir)
  }

  private def evaluate(config: Config) {
    object ::> {def unapply[T] (arr: Array[T]) = Some((arr.init, arr.last))}
    val ruleStats = Source.fromFile(config.inputRules).getLines()
      .map(rule => rule.split(" ") match {
        case Array(id, _*) ::> supp ::> conf => (id.toInt -> conf.toDouble)})
      .toMap

    val inferFacts = spark.textFile(config.inputFacts, 64)
      .filter(line => !line.split(':')(1).split(' ').contains("0"))
      .map(line => line.split(':') match {
        case Array(fact, rules) => (
          fact.split(" ") match {
              case Array(p, x, y) => (p.toInt, x.toInt, y.toInt)},
          rules.split(" ").map(rule => ruleStats(rule.toInt)).max
        )})
      .reduceByKey((a, b) => Math.max(a, b), 512)

    val preds = Source.fromFile(config.inputRules).getLines()
      .map(rule => rule.split(" ") match {
        case Array(id, h, _*) => h.toInt}).toSet

    val truths = spark.textFile(config.trueFacts, 64)
      .map(fact => fact.split(" ") match {
        case Array(p, x, y) => ((p.toInt, x.toInt, y.toInt), 0)})
      .filter({case ((p, x, y), 0) => preds.contains(p)})
      .rightOuterJoin(inferFacts, 10000)
      .map({
        case ((p, x, y), (Some(_), conf)) => (p, x, y, conf, 1, 1)
        case ((p, x, y), (None, conf)) => (p, x, y, conf, 0, 1)
      })
      // .sortBy({case (p, x, y, conf, _, _) => conf}, false, 10000)
      .map(_.productIterator.mkString(" "))
      .saveAsTextFile(config.outputDir)
  }

  private def evaluate2(config: Config) {
    object ::> {def unapply[T] (arr: Array[T]) = Some((arr.init, arr.last))}
    val ruleStats = spark.broadcast(
      Source.fromFile(config.inputRules).getLines()
      .map(rule => rule.split(" ") match {
        case Array(id, _*) ::> supp ::> conf => (id.toInt -> conf.toDouble)})
      .toMap)

    val inferFacts = spark.textFile(config.inputFacts, 64)
      .map(line => line.split(':') match {
        case Array(fact, rules) => (fact.split(' ') match {
          case Array(p, x, y) => (p.toInt, x.toInt, y.toInt)}, rules)
      })
      .filter({case (fact, rules) => !rules.split(' ').contains("0")})

    val preds = spark.broadcast(Source.fromFile(config.inputRules).getLines()
      .map(rule => rule.split(" ") match {
        case Array(id, h, _*) => h.toInt}).toSet)

    val truths = spark.textFile(config.trueFacts, 64)
      .map(fact => fact.split(" ") match {
        case Array(p, x, y) => ((p.toInt, x.toInt, y.toInt), 0)})
      .filter({case ((p, x, y), 0) => preds.value.contains(p)})
      .rightOuterJoin(inferFacts, 512)
      .map({
        case ((p, x, y), (Some(_), rules)) => (rules, 1, 1)
        case ((p, x, y), (None, rules)) => (rules, 0, 1)
      })
      .flatMap({case (rules, sum, cnt) => rules.split(' ')
                  .map(r => (r.toInt, (sum, cnt)))})
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map({case (r, (sum, cnt)) => (r, 1.0*sum/cnt, ruleStats.value(r.toInt))})
      .sortBy({case (r, conf, _) => conf}, false, 8000)
      .map(_.productIterator.mkString(" "))
      .saveAsTextFile(config.outputDir)
  }

  // Runs the partitioning algorithm.
  private def partition(config: Config) {
    val rules = Source.fromFile(config.inputRules).getLines()
      .map(rule => rule.split(" ").map(_.toInt))
      .toArray

    val histogram = spark.textFile(config.inputFacts, 64)
      .map(f => f.split(" ") match {
        case Array(pred, sub, obj) => (pred.toInt, 1)})
      .reduceByKey(_ + _)
      .collectAsMap

    val partitions = new Partitioner(rules, histogram,
        config.maxFacts, config.maxRules).getPartitions
    writePartitions(partitions, config)
  }

  private def writePartitions(partitions: Array[Partition],
                              config: Config) {
    for ((partition, index) <- partitions.zipWithIndex) {
      val predicates = spark.broadcast(partition.predicates)
      val facts = spark.textFile(config.inputFacts, 64)
        .map(fact => fact.split(" ") map(_.toInt))
        .filter(fact => predicates.value.contains(fact(0)))
        .map(fact => fact.mkString(" "))
        .saveAsTextFile(config.outputDir + "/part-" + index + "/" +
                        config.outputFacts)

      val rules = new java.io.PrintWriter(
        config.outputDir + "/part-" + index + "/" + config.outputRules)
      rules.write(partition.rules.map(rule => rule.mkString(" "))
           .mkString("\n"))
      rules.close
    }
  }
  
  private val OPLearners = Array(
    OPLearnerType1, OPLearnerType1, OPLearnerType2, OPLearnerType3,
    OPLearnerType4, OPLearnerType5, OPLearnerType6, OPLearnerType7,
    OPLearnerType8, OPLearnerType9
  )
}
