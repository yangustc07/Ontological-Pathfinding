import org.apache.spark.rdd.RDD;
import org.apache.spark.broadcast.Broadcast
import scala.collection.Iterable;
import scala.collection.mutable.HashSet;

object OPLearnerType1 extends OPLearnerBase {
  protected override def applyRules(
    facts: RDD[Array[Int]], rules: Broadcast[Array[Array[Int]]],
    constraint: Int) = { 
    val rulesTable = rules.value.groupBy({case Array(id, head, body) => body})

    facts.flatMap({case Array(pred, sub, obj) =>
      for (Array(id, head, body) <-
           rulesTable.getOrElse(pred, Array.empty[Array[Int]]))
        yield ((head, sub, obj), id)})
    .union(
      facts.map({case Array(pred, sub, obj) => ((pred, sub, obj), BASE_FACT)})
    )
    .aggregateByKey(new HashSet[Int], NUM_CHECK_TASKS)(_ += _, _ ++= _)
  }
}

object OPLearnerType2 extends OPLearnerBase {
  protected override def applyRules(
    facts: RDD[Array[Int]], rules: Broadcast[Array[Array[Int]]],
    constraint: Int) = { 
    val rulesTable = rules.value.groupBy({case Array(id, head, body) => body})

    facts.flatMap({case Array(pred, sub, obj) =>
      for (Array(id, head, body) <-
           rulesTable.getOrElse(pred, Array.empty[Array[Int]]))
        yield ((head, obj, sub), id)})
    .union(
      facts.map({case Array(pred, sub, obj) => ((pred, sub, obj), BASE_FACT)})
    )
    .aggregateByKey(new HashSet[Int], NUM_CHECK_TASKS)(_ += _, _ ++= _)
  }
}

object OPLearnerType3 extends OPLearnerBase {
  protected override def applyRules(
    facts: RDD[Array[Int]], rules: Broadcast[Array[Array[Int]]],
    constraint: Int) = { 
    facts.map({case Array(pred, sub, obj) => (sub, (pred, obj))})  
      .groupByKey(NUM_JOIN_TASKS)
      .flatMap({case (sub, facts) => join(sub, facts, rules.value, constraint)})
      .aggregateByKey(new HashSet[Int], NUM_CHECK_TASKS)(_ += _, _ ++= _)
  }

  private def join(sub: Int, facts: Iterable[(Int, Int)],
                   rules: Array[Array[Int]], constraint: Int) = { 
    val predMap = facts.groupBy(_._1)
    val (filteredRules, candRules) = rules.partition(
      {case Array(id, head, body1, body2) =>
         predMap.getOrElse(body1, Nil).size > constraint &&
         predMap.getOrElse(body2, Nil).size > constraint})
    filteredRules.foreach(rule => println(rule(0)))

    (for (Array(id, head, body1, body2) <- candRules;
          (pred1, obj1) <- predMap.getOrElse(body1, Nil);
          (pred2, obj2) <- predMap.getOrElse(body2, Nil))
      yield ((head, obj1 /* subject */, obj2 /* object */), id)) ++ (
    facts.map({case (pred, obj) => ((pred, sub, obj), BASE_FACT)}))
  }
}

object OPLearnerType4 extends OPLearnerBase {
  protected override def applyRules(
    facts: RDD[Array[Int]], rules: Broadcast[Array[Array[Int]]],
    constraint: Int) = {
    val facts1 = facts.map({case Array(pred, sub, obj) => (obj, (pred, sub))})
    val facts2 = facts.map({case Array(pred, sub, obj) => (sub, (pred, obj))})
    facts1.cogroup(facts2, NUM_JOIN_TASKS)
      .flatMap({case (obj, (f1, f2)) =>
          join(obj, f1, f2, rules.value, constraint)})
      .aggregateByKey(new HashSet[Int], NUM_CHECK_TASKS)(_ += _, _ ++= _)
  }

  private def join(obj: Int, facts1: Iterable[(Int, Int)],
                   facts2: Iterable[(Int, Int)], rules: Array[Array[Int]],
                   constraint: Int) = {
    val predMap1 = facts1.groupBy(_._1)
    val predMap2 = facts2.groupBy(_._1)
    val (filteredRules, candRules) = rules.partition(
      {case Array(id, head, body1, body2) =>
         predMap1.getOrElse(body1, Nil).size > constraint &&
         predMap2.getOrElse(body2, Nil).size > constraint})
    filteredRules.foreach(rule => println(rule(0)))

    (for (Array(id, head, body1, body2) <- candRules;
          (pred1, sub1) <- predMap1.getOrElse(body1, Nil);
          (pred2, obj2) <- predMap2.getOrElse(body2, Nil))
      yield ((head, sub1 /* subject */, obj2 /* object */), id)) ++ (
    facts1.map({case (pred, sub) => ((pred, sub, obj), BASE_FACT)}))
  }
}

object OPLearnerType5 extends OPLearnerBase {
  protected override def applyRules(
    facts: RDD[Array[Int]], rules: Broadcast[Array[Array[Int]]],
    constraint: Int) = {
    val facts1 = facts.map({case Array(pred, sub, obj) => (sub, (pred, obj))})
    val facts2 = facts.map({case Array(pred, sub, obj) => (obj, (pred, sub))})
    facts1.cogroup(facts2, NUM_JOIN_TASKS)
      .flatMap({case (sub, (f1, f2)) =>
          join(sub, f1, f2, rules.value, constraint)})
      .aggregateByKey(new HashSet[Int], NUM_CHECK_TASKS)(_ += _, _ ++= _)
  }

  private def join(sub: Int, facts1: Iterable[(Int, Int)],
                   facts2: Iterable[(Int, Int)], rules: Array[Array[Int]],
                   constraint: Int) = {
    val predMap1 = facts1.groupBy(_._1)
    val predMap2 = facts2.groupBy(_._1)
    val (filteredRules, candRules) = rules.partition(
      {case Array(id, head, body1, body2) =>
         predMap1.getOrElse(body1, Nil).size > constraint &&
         predMap2.getOrElse(body2, Nil).size > constraint})
    filteredRules.foreach(rule => println(rule(0)))

    (for (Array(id, head, body1, body2) <- candRules;
          (pred1, obj1) <- predMap1.getOrElse(body1, Nil);
          (pred2, sub2) <- predMap2.getOrElse(body2, Nil))
      yield ((head, obj1 /* subject */, sub2 /* object */), id)) ++ (
    facts1.map({case (pred, obj) => ((pred, sub, obj), BASE_FACT)}))
  }
}

object OPLearnerType6 extends OPLearnerBase {
  protected override def applyRules(
    facts: RDD[Array[Int]], rules: Broadcast[Array[Array[Int]]],
    constraint: Int) = {
    facts.map({case Array(pred, sub, obj) => (obj, (pred, sub))})  
      .groupByKey(NUM_JOIN_TASKS)  // group by subject
      .flatMap({case (obj, facts) => join(obj, facts, rules.value, constraint)})
      .aggregateByKey(new HashSet[Int], NUM_CHECK_TASKS)(_ += _, _ ++= _)
  }

  private def join(obj: Int, facts: Iterable[(Int, Int)],
                   rules: Array[Array[Int]], constraint: Int) = { 
    val predMap = facts.groupBy(_._1)
    val (filteredRules, candRules) = rules.partition(
      {case Array(id, head, body1, body2) =>
         predMap.getOrElse(body1, Nil).size > constraint &&
         predMap.getOrElse(body2, Nil).size > constraint})
    filteredRules.foreach(rule => println(rule(0)))

    (for (Array(id, head, body1, body2) <- candRules;
          (pred1, sub1) <- predMap.getOrElse(body1, Nil);
          (pred2, sub2) <- predMap.getOrElse(body2, Nil))
      yield ((head, sub1 /* subject */, sub2 /* object */), id)) ++ (
    facts.map({case (pred, sub) => ((pred, sub, obj), BASE_FACT)}))
  }
}

object OPLearnerType7 extends OPLearnerBase {
  protected override def applyRules(
    facts: RDD[Array[Int]], rules: Broadcast[Array[Array[Int]]],
    constraint: Int) = {
    val facts1 = facts.map({case Array(pred, sub, obj) => (obj, (pred, sub))})
    val facts2 = facts.map({case Array(pred, sub, obj) => (sub, (pred, obj))})
    val facts3 = facts.map({case Array(pred, sub, obj) => (sub, (pred, obj))})
    facts1.cogroup(facts2, NUM_JOIN_TASKS)
      .flatMap({case (obj, (f1, f2)) => join1(f1, f2, rules.value, constraint)})
      .cogroup(facts3, NUM_JOIN_TASKS)
      .flatMap({case (sub, (j1, f3)) =>
          join2(sub, j1, f3, rules.value, constraint)})
      .aggregateByKey(new HashSet[Int], NUM_CHECK_TASKS)(_ += _, _ ++= _)
  }

  private def join1(facts1: Iterable[(Int, Int)], facts2: Iterable[(Int, Int)],
                    rules: Array[Array[Int]], constraint: Int) = { 
    val predMap1 = facts1.groupBy(_._1)
    val predMap2 = facts2.groupBy(_._1)
    val (filteredRules, candRules) = rules.partition(
      {case Array(id, _, body1, body2, _) =>
         predMap1.getOrElse(body1, Nil).size > constraint &&
         predMap2.getOrElse(body2, Nil).size > constraint})
    filteredRules.foreach(rule => println(rule(0)))

    for (Array(id, _, body1, body2, _) <- candRules;
          (pred1, sub1) <- predMap1.getOrElse(body1, Nil);
          (pred2, obj2) <- predMap2.getOrElse(body2, Nil))
      yield (obj2, (id, sub1))
  }

  private def join2(sub: Int, j1: Iterable[(Int, Int)],
                    facts3: Iterable[(Int, Int)], rules: Array[Array[Int]],
                    constraint: Int) = {
    val joinMap1 = j1.groupBy(_._1)
    val predMap2 = facts3.groupBy(_._1)
    val (filteredRules, candRules) = rules.partition(
      {case Array(id, head, _, _, body3) =>
         joinMap1.getOrElse(id, Nil).size > constraint &&
         predMap2.getOrElse(body3, Nil).size > constraint})
    filteredRules.foreach(rule => println(rule(0)))

    (for (Array(id, head, _, _, body3) <- candRules;
          (rid, sub1) <- joinMap1.getOrElse(id, Nil);
          (pred3, obj3) <- predMap2.getOrElse(body3, Nil))
       yield ((head, sub1, obj3), id)) ++ (
     facts3.map({case (pred, obj) => ((pred, sub, obj), BASE_FACT)}))
  }
}

object OPLearnerType8 extends OPLearnerBase {
  protected override def applyRules(
    facts: RDD[Array[Int]], rules: Broadcast[Array[Array[Int]]],
    constraint: Int) = {
    val facts1 = facts.map({case Array(pred, sub, obj) => (obj, (pred, sub))})
    val facts2 = facts.map({case Array(pred, sub, obj) => (sub, (pred, obj))})
    val facts3 = facts.map({case Array(pred, sub, obj) => (sub, (pred, obj))})
    val facts4 = facts.map({case Array(pred, sub, obj) => (sub, (pred, obj))})
    facts1.cogroup(facts2, NUM_JOIN_TASKS)
      .flatMap({case (obj, (f1, f2)) => join1(f1, f2, rules.value, constraint)})
      .cogroup(facts3, NUM_JOIN_TASKS)
      .flatMap({case (sub, (j1, f3)) => join2(j1, f3, rules.value, constraint)})
      .cogroup(facts4, NUM_JOIN_TASKS)
      .flatMap({case (sub, (j1, f4)) =>
          join3(sub, j1, f4, rules.value, constraint)})
      .aggregateByKey(new HashSet[Int], NUM_CHECK_TASKS)(_ += _, _ ++= _)
  }

  private def join1(facts1: Iterable[(Int, Int)], facts2: Iterable[(Int, Int)],
                    rules: Array[Array[Int]], constraint: Int) = { 
    val predMap1 = facts1.groupBy(_._1)
    val predMap2 = facts2.groupBy(_._1)
    val (filteredRules, candRules) = rules.partition(
      {case Array(id, _, body1, body2, _*) =>
         predMap1.getOrElse(body1, Nil).size > constraint &&
         predMap2.getOrElse(body2, Nil).size > constraint})
    filteredRules.foreach(rule => println(rule(0)))

    for (Array(id, _, body1, body2, _*) <- candRules;
          (pred1, sub1) <- predMap1.getOrElse(body1, Nil);
          (pred2, obj2) <- predMap2.getOrElse(body2, Nil))
      yield (obj2, (id, sub1))
  }

  private def join2(j1: Iterable[(Int, Int)], facts3: Iterable[(Int, Int)],
                    rules: Array[Array[Int]], constraint: Int) = {
    val joinMap1 = j1.groupBy(_._1)
    val predMap2 = facts3.groupBy(_._1)
    val (filteredRules, candRules) = rules.partition(
      {case Array(id, _, _, _, body3, _) =>
         joinMap1.getOrElse(id, Nil).size > constraint &&
         predMap2.getOrElse(body3, Nil).size > constraint})
    filteredRules.foreach(rule => println(rule(0)))

    for (Array(id, _, _, _, body3, _) <- candRules;
          (rid, sub1) <- joinMap1.getOrElse(id, Nil);
          (pred3, obj3) <- predMap2.getOrElse(body3, Nil))
       yield (obj3, (rid, sub1))
  }

  private def join3(sub: Int, j2: Iterable[(Int, Int)],
                    facts4: Iterable[(Int, Int)], rules: Array[Array[Int]],
                    constraint: Int) = {
    val joinMap1 = j2.groupBy(_._1)
    val predMap2 = facts4.groupBy(_._1)
    val (filteredRules, candRules) = rules.partition(
      {case Array(id, head, _, _, _, body4) =>
         joinMap1.getOrElse(id, Nil).size > constraint &&
         predMap2.getOrElse(body4, Nil).size > constraint})
    filteredRules.foreach(rule => println(rule(0)))

    (for (Array(id, head, _, _, _, body4) <- candRules;
          (rid, sub1) <- joinMap1.getOrElse(id, Nil);
          (pred4, obj4) <- predMap2.getOrElse(body4, Nil))
       yield ((head, sub1, obj4), id)) ++ (
     facts4.map({case (pred, obj) => ((pred, sub, obj), BASE_FACT)}))
  }
}

object OPLearnerType9 extends OPLearnerBase {
  protected override def applyRules(
    facts: RDD[Array[Int]], rules: Broadcast[Array[Array[Int]]],
    constraint: Int) = {
    val facts1 = facts.map({case Array(pred, sub, obj) => (obj, (pred, sub))})
    val facts2 = facts.map({case Array(pred, sub, obj) => (sub, (pred, obj))})
    val facts3 = facts.map({case Array(pred, sub, obj) => (sub, (pred, obj))})
    val facts4 = facts.map({case Array(pred, sub, obj) => (sub, (pred, obj))})
    val facts5 = facts.map({case Array(pred, sub, obj) => (sub, (pred, obj))})
    facts1.cogroup(facts2, NUM_JOIN_TASKS)
      .flatMap({case (obj, (f1, f2)) => join1(f1, f2, rules.value, constraint)})
      .cogroup(facts3, NUM_JOIN_TASKS)
      .flatMap({case (sub, (j1, f3)) => join2(j1, f3, rules.value, constraint)})
      .cogroup(facts4, NUM_JOIN_TASKS)
      .flatMap({case (sub, (j2, f4)) => join3(j2, f4, rules.value, constraint)})
      .cogroup(facts5, NUM_JOIN_TASKS)
      .flatMap({case (sub, (j3, f5)) =>
          join4(sub, j3, f5, rules.value, constraint)})
      .aggregateByKey(new HashSet[Int], NUM_CHECK_TASKS)(_ += _, _ ++= _)
  }

  private def join1(facts1: Iterable[(Int, Int)], facts2: Iterable[(Int, Int)],
                    rules: Array[Array[Int]], constraint: Int) = { 
    val predMap1 = facts1.groupBy(_._1)
    val predMap2 = facts2.groupBy(_._1)
    val (filteredRules, candRules) = rules.partition(
      {case Array(id, _, body1, body2, _*) =>
         predMap1.getOrElse(body1, Nil).size > constraint &&
         predMap2.getOrElse(body2, Nil).size > constraint})
    filteredRules.foreach(rule => println(rule(0)))

    for (Array(id, _, body1, body2, _*) <- candRules;
          (pred1, sub1) <- predMap1.getOrElse(body1, Nil);
          (pred2, obj2) <- predMap2.getOrElse(body2, Nil))
      yield (obj2, (id, sub1))
  }

  private def join2(j1: Iterable[(Int, Int)], facts3: Iterable[(Int, Int)],
                    rules: Array[Array[Int]], constraint: Int) = {
    val joinMap1 = j1.groupBy(_._1)
    val predMap2 = facts3.groupBy(_._1)
    val (filteredRules, candRules) = rules.partition(
      {case Array(id, _, _, _, body3, _*) =>
         joinMap1.getOrElse(id, Nil).size > constraint &&
         predMap2.getOrElse(body3, Nil).size > constraint})
    filteredRules.foreach(rule => println(rule(0)))

    for (Array(id, _, _, _, body3, _*) <- candRules;
          (rid, sub1) <- joinMap1.getOrElse(id, Nil);
          (pred3, obj3) <- predMap2.getOrElse(body3, Nil))
       yield (obj3, (rid, sub1))
  }

  private def join3(j2: Iterable[(Int, Int)], facts4: Iterable[(Int, Int)],
                    rules: Array[Array[Int]], constraint: Int) = {
    val joinMap1 = j2.groupBy(_._1)
    val predMap2 = facts4.groupBy(_._1)
    val (filteredRules, candRules) = rules.partition(
      {case Array(id, _, _, _, _, body4, _*) =>
         joinMap1.getOrElse(id, Nil).size > constraint &&
         predMap2.getOrElse(body4, Nil).size > constraint})
    filteredRules.foreach(rule => println(rule(0)))

    for (Array(id, _, _, _, _, body4, _*) <- candRules;
          (rid, sub1) <- joinMap1.getOrElse(id, Nil);
          (pred4, obj4) <- predMap2.getOrElse(body4, Nil))
       yield (obj4, (rid, sub1))
  }

  private def join4(sub: Int, j3: Iterable[(Int, Int)],
                    facts5: Iterable[(Int, Int)], rules: Array[Array[Int]],
                    constraint: Int) = {
    val joinMap1 = j3.groupBy(_._1)
    val predMap2 = facts5.groupBy(_._1)
    val (filteredRules, candRules) = rules.partition(
      {case Array(id, head, _, _, _, _, body5) =>
         joinMap1.getOrElse(id, Nil).size > constraint &&
         predMap2.getOrElse(body5, Nil).size > constraint})
    filteredRules.foreach(rule => println(rule(0)))

    (for (Array(id, head, _, _, _, _, body5) <- candRules;
          (rid, sub1) <- joinMap1.getOrElse(id, Nil);
          (pred5, obj5) <- predMap2.getOrElse(body5, Nil))
       yield ((head, sub1, obj5), id)) ++ (
     facts5.map({case (pred, obj) => ((pred, sub, obj), BASE_FACT)}))
  }
}
