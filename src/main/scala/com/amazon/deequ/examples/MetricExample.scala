package com.amazon.deequ.examples

import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.analyzers._
import com.amazon.deequ.examples.ExampleUtils.{itemsAsDataframe, withSpark}

/**
 * @author TomAndersen
 */
object MetricExample {
  def main(args: Array[String]): Unit = {

    withSpark { session =>
      val data = itemsAsDataframe(
        session,
        Item(1, "Thingy A", "10409983787", "high", 0),
        Item(2, "Thingy B", "13409983787", "low", 0),
        Item(3, "欧阳娜娜", null, null, 5),
        Item(4, "王志开", null, "null", 10),
        Item(5, "黄志远", "110", "null", 10),
      )

      val analysis = Analysis()
        .addAnalyzer(Size())
        .addAnalyzer(ApproxCountDistinct("id"))
        .addAnalyzer(Completeness("productName"))
        .addAnalyzer(Completeness("description"))
        .addAnalyzer(EmptyRatio("description"))
        .addAnalyzer(EmptySize("description"))
        .addAnalyzer(PatternMatchRatio("productName", Patterns.CHINESE_NAME))
        .addAnalyzer(PatternMatchSize("productName", Patterns.CHINESE_NAME))
        .addAnalyzer(PatternNotMatchRatio("productName", Patterns.CHINESE_NAME))
        .addAnalyzer(PatternNotMatchSize("productName", Patterns.CHINESE_NAME))
        .addAnalyzer(PatternMatchSize("description", Patterns.CHINESE_PHONE))

      //      val stateStore = InMemoryStateProvider()

      val metricsForData = AnalysisRunner.run(
        data = data,
        analysis = analysis,
        //        saveStatesWith = Some(stateStore) // persist the internal state of the computation
      )

      println(s"Metrics for the first ${data.count()} records:\n")
      metricsForData.metricMap.foreach {
        case (analyzer, metric)
        => println(s"\t$analyzer: ${metric.value.get}")
      }

    }
  }
}
