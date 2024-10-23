/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.analyzers.runners

import com.amazon.deequ.analyzers._
import com.amazon.deequ.io.DfsUtils
import com.amazon.deequ.metrics.{DoubleMetric, Metric}
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import scala.util.Success

private[deequ] case class AnalysisRunnerRepositoryOptions(
      metricsRepository: Option[MetricsRepository] = None,
      // 此处的数据类型存在问题, 应该设置成 Seq[Option[ResultKey]], 因为之前的计算结果可能对应多个ResultKey
      reuseExistingResultsForKey: Option[ResultKey] = None,
      failIfResultsForReusingMissing: Boolean = false,
      saveOrAppendResultsWithKey: Option[ResultKey] = None)

private[deequ] case class AnalysisRunnerFileOutputOptions(
      sparkSession: Option[SparkSession] = None,
      saveSuccessMetricsJsonToPath: Option[String] = None,
      overwriteOutputFiles: Boolean = false)

/**
  * Runs a set of analyzers on the data at hand and optimizes the resulting computations to minimize
  * the number of scans over the data. Additionally, the internal states of the computation can be
  * stored and aggregated with existing states to enable incremental computations.
  */
object AnalysisRunner {

   /**
    * Starting point to construct an AnalysisRun.
    *
    * @param data tabular data on which the checks should be verified
    */
  def onData(data: DataFrame): AnalysisRunBuilder = {
    new AnalysisRunBuilder(data)
  }

  /**
    * Compute the metrics from the analyzers configured in the analyis
    *
    * @param data data on which to operate
    * @param analysis analysis defining the analyzers to run
    * @param aggregateWith load existing states for the configured analyzers and aggregate them
    *                      (optional)
    * @param saveStatesWith persist resulting states for the configured analyzers (optional)
    * @param storageLevelOfGroupedDataForMultiplePasses caching level for grouped data that must be
    *                                                   accessed multiple times (use
    *                                                   StorageLevel.NONE to completely disable
    *                                                   caching)
    * @return AnalyzerContext holding the requested metrics per analyzer
    */
  def run(
      data: DataFrame,
      analysis: Analysis,
      aggregateWith: Option[StateLoader] = None,
      saveStatesWith: Option[StatePersister] = None,
      storageLevelOfGroupedDataForMultiplePasses: StorageLevel = StorageLevel.MEMORY_AND_DISK)
    : AnalyzerContext = {

    doAnalysisRun(data, analysis.analyzers, aggregateWith, saveStatesWith,
      storageLevelOfGroupedDataForMultiplePasses)
  }

  /**
    * Compute the metrics from the analyzers configured in the analyis
    *
    * @param data data on which to operate
    * @param analyzers the analyzers to run
    * @param aggregateWith load existing states for the configured analyzers and aggregate them
    *                      (optional)
    * @param saveStatesWith persist resulting states for the configured analyzers (optional)
    * @param storageLevelOfGroupedDataForMultiplePasses caching level for grouped data that must be
    *                                                   accessed multiple times (use
    *                                                   StorageLevel.NONE to completely disable
    *                                                   caching)
    * @param metricsRepositoryOptions Options related to the MetricsRepository
    * @param fileOutputOptions Options related to FileOuput using a SparkSession
    * @return AnalyzerContext holding the requested metrics per analyzer
    */
  private[deequ] def doAnalysisRun(
      data: DataFrame,
      analyzers: Seq[Analyzer[_, Metric[_]]],
      aggregateWith: Option[StateLoader] = None,
      saveStatesWith: Option[StatePersister] = None,
      storageLevelOfGroupedDataForMultiplePasses: StorageLevel = StorageLevel.MEMORY_AND_DISK,
      metricsRepositoryOptions: AnalysisRunnerRepositoryOptions =
        AnalysisRunnerRepositoryOptions(),
      fileOutputOptions: AnalysisRunnerFileOutputOptions =
        AnalysisRunnerFileOutputOptions())
    : AnalyzerContext = {

    if (analyzers.isEmpty) {
      return AnalyzerContext.empty
    }

    // 更换 Analyzer 对象的变量类型, 并判断是否有重复 Analyzer, 如果存在重复的Analyzer则抛异常
    // 此代码其实说明变量传入时 Analyzer 类型的声明不合适, 应该设置为 Seq[Analyzer[State[_], Metric[_]]]
    val allAnalyzers = analyzers.map { _.asInstanceOf[Analyzer[State[_], Metric[_]]] }
    val distinctAnalyzers = allAnalyzers.distinct
    require(distinctAnalyzers.size == allAnalyzers.size,
      s"Duplicate analyzers found: ${allAnalyzers.diff(distinctAnalyzers).distinct}")

    /* We do not want to recalculate calculated metrics in the MetricsRepository */
    // 加载之前已经计算过的 Analysis 结果, 即 AnalyzerContext
    val resultsComputedPreviously: AnalyzerContext =
      (metricsRepositoryOptions.metricsRepository,
        metricsRepositoryOptions.reuseExistingResultsForKey)
        match {
          case (Some(metricsRepository: MetricsRepository), Some(resultKey: ResultKey)) =>
            metricsRepository.loadByKey(resultKey).getOrElse(AnalyzerContext.empty)
          case _ => AnalyzerContext.empty
        }

    // 剔除已经运行过的 Analyzer, 获取还未运行的 Analyzer
    val analyzersAlreadyRan = resultsComputedPreviously.metricMap.keys.toSet
    val analyzersToRun = allAnalyzers.filterNot(analyzersAlreadyRan.contains)

    // 判断所有 Metric 是否都从之前的 Repository 中获取, 以及是否都获取成功
    /* Throw an error if all needed metrics should have gotten calculated before but did not */
    if (metricsRepositoryOptions.failIfResultsForReusingMissing && analyzersToRun.nonEmpty) {
      throw new ReusingNotPossibleResultsMissingException(
        "Could not find all necessary results in the MetricsRepository, the calculation of " +
          s"the metrics for these analyzers would be needed: ${analyzersToRun.mkString(", ")}")
    }

    // 校验所有待执行的 Analyzer 的 precondition 函数, 即判断数据集的 schema 是否满足这些前置条件
    /* Find all analyzers which violate their preconditions */
    val passedAnalyzers = analyzersToRun
      .filter { analyzer =>
        Preconditions.findFirstFailing(data.schema, analyzer.preconditions).isEmpty
      }

    // 获取不满足 precondition 的 Analyzer
    val failedAnalyzers = analyzersToRun.diff(passedAnalyzers)
    // 基于 precondition 执行失败的原因, 生成对应的失败 Metric, 并和对应的 Analyzer 组装成 AnalyzerContext
    /* Create the failure metrics from the precondition violations */
    val preconditionFailures = computePreconditionFailureMetrics(failedAnalyzers, data.schema)

    // 将通过 precondition 的 Analyzer 按照是否属于 GroupingAnalyzer 类型, 拆分为两个集合
    /* Identify analyzers which require us to group the data */
    val (groupingAnalyzers, allScanningAnalyzers) =
      passedAnalyzers.partition { _.isInstanceOf[GroupingAnalyzer[State[_], Metric[_]]] }

    // 将 scanningAnalyzers 按照是否属于 KLLSketch Analyzer 拆分为两个集合
    val (kllAnalyzers, scanningAnalyzers) =
      allScanningAnalyzers.partition { _.isInstanceOf[KLLSketch] }

    // 执行 kllAnalyzers, 并获取其对应的执行结果 AnalyzerContext
    val kllMetrics =
      if (kllAnalyzers.nonEmpty) {
        KLLRunner.computeKLLSketchesInExtraPass(data, kllAnalyzers, aggregateWith, saveStatesWith)
      } else {
        AnalyzerContext.empty
      }

    // 执行 scanningAnalyzers, 并获取其对应的执行结果 AnalyzerContext
    /* Run the analyzers which do not require grouping in a single pass over the data */
    val nonGroupedMetrics =
      runScanningAnalyzers(data, scanningAnalyzers, aggregateWith, saveStatesWith)

    // 从计算结果中获取 Size() Analyzer 的计算结果, 即获取数据的行数
    // TODO this can be further improved, we can get the number of rows from other metrics as well
    // TODO we could also insert an extra Size() computation if we have to scan the data anyways
    var numRowsOfData = nonGroupedMetrics.metric(Size()).collect {
      case DoubleMetric(_, _, _, Success(value: Double)) => value.toLong
    }

    var groupedMetrics = AnalyzerContext.empty

    // 将 groupingAnalyzers 按照 group 列和 where 条件进行分组并执行, 返回结果填充到 AnalyzerContext 中
    /* Run grouping analyzers based on the columns which they need to group on */
    groupingAnalyzers
      .map { _.asInstanceOf[GroupingAnalyzer[State[_], Metric[_]]] }
      .groupBy { a => (a.groupingColumns().sorted, getFilterCondition(a)) }
      .foreach { case ((groupingColumns, filterCondition), analyzersForGrouping) =>

        val (numRows, metrics) =
          runGroupingAnalyzers(data, groupingColumns, filterCondition, analyzersForGrouping,
            aggregateWith, saveStatesWith, storageLevelOfGroupedDataForMultiplePasses,
            numRowsOfData)

        groupedMetrics = groupedMetrics ++ metrics

        /* if we don't know the size of the data yet, we know it after the first pass */
        if (numRowsOfData.isEmpty) {
          numRowsOfData = Option(numRows)
        }
      }

    // 将所有 AnalyzerContext 拼接
    val resultingAnalyzerContext = resultsComputedPreviously ++ preconditionFailures ++
      nonGroupedMetrics ++ groupedMetrics ++ kllMetrics

    // 如果 ResultKey 和 Repository 不为空, 则将本次计算结果写入 Repository 中
    saveOrAppendResultsIfNecessary(
      resultingAnalyzerContext,
      metricsRepositoryOptions.metricsRepository,
      metricsRepositoryOptions.saveOrAppendResultsWithKey)
    // 如果 fileOutputOptions 不为空, 则将本次计算结果写入对应的JSON文件
    saveJsonOutputsToFilesystemIfNecessary(fileOutputOptions, resultingAnalyzerContext)

    resultingAnalyzerContext
  }

  private[this] def getFilterCondition(analyzer: Analyzer[State[_], Metric[_]]): Option[String] = {
    analyzer match {
      case a : FilterableAnalyzer => a.filterCondition
      case _ => None
    }
  }

  private[this] def saveOrAppendResultsIfNecessary(
      resultingAnalyzerContext: AnalyzerContext,
      metricsRepository: Option[MetricsRepository],
      saveOrAppendResultsWithKey: Option[ResultKey])
    : Unit = {

    metricsRepository.foreach { repository =>
      saveOrAppendResultsWithKey.foreach { key =>

        val currentValueForKey = repository.loadByKey(key).getOrElse(AnalyzerContext.empty)

        // AnalyzerContext entries on the right side of ++ will overwrite the ones on the left
        // if there are two different metric results for the same analyzer
        val valueToSave = currentValueForKey ++ resultingAnalyzerContext

        repository.save(saveOrAppendResultsWithKey.get, valueToSave)
      }
    }
  }

  private[this] def saveJsonOutputsToFilesystemIfNecessary(
    fileOutputOptions: AnalysisRunnerFileOutputOptions,
    analyzerContext: AnalyzerContext)
  : Unit = {

    fileOutputOptions.sparkSession.foreach { session =>
      fileOutputOptions.saveSuccessMetricsJsonToPath.foreach { profilesOutput =>

        DfsUtils.writeToTextFileOnDfs(session, profilesOutput,
          overwrite = fileOutputOptions.overwriteOutputFiles) { writer =>
            writer.append(AnalyzerContext.successMetricsAsJson(analyzerContext))
            writer.newLine()
          }
        }
    }
  }

  private[this] def computePreconditionFailureMetrics(
      failedAnalyzers: Seq[Analyzer[State[_], Metric[_]]],
      schema: StructType)
    : AnalyzerContext = {

    val failures = failedAnalyzers.map { analyzer =>

      val firstException = Preconditions
        .findFirstFailing(schema, analyzer.preconditions).get

      analyzer -> analyzer.toFailureMetric(firstException)
    }
    .toMap[Analyzer[_, Metric[_]], Metric[_]]

    AnalyzerContext(failures)
  }

  private[this] def runGroupingAnalyzers(
      data: DataFrame,
      groupingColumns: Seq[String],
      filterCondition: Option[String],
      analyzers: Seq[GroupingAnalyzer[State[_], Metric[_]]],
      aggregateWith: Option[StateLoader],
      saveStatesTo: Option[StatePersister],
      storageLevelOfGroupedDataForMultiplePasses: StorageLevel,
      numRowsOfData: Option[Long])
    : (Long, AnalyzerContext) = {

    /* Compute the frequencies of the request groups once */
    var frequenciesAndNumRows = FrequencyBasedAnalyzer.computeFrequencies(data, groupingColumns,
      filterCondition)

    /* Pick one analyzer to store the state for */
    val sampleAnalyzer = analyzers.head.asInstanceOf[Analyzer[FrequenciesAndNumRows, Metric[_]]]

    /* Potentially aggregate states */
    aggregateWith
      .foreach { _.load[FrequenciesAndNumRows](sampleAnalyzer)
        .foreach { previousFrequenciesAndNumRows =>
          frequenciesAndNumRows = frequenciesAndNumRows.sum(previousFrequenciesAndNumRows)
        }
      }

    val results = runAnalyzersForParticularGrouping(frequenciesAndNumRows, analyzers, saveStatesTo,
        storageLevelOfGroupedDataForMultiplePasses)

    frequenciesAndNumRows.numRows -> results
  }

  private[this] def runScanningAnalyzers(
      data: DataFrame,
      analyzers: Seq[Analyzer[State[_], Metric[_]]],
      aggregateWith: Option[StateLoader] = None,
      saveStatesTo: Option[StatePersister] = None)
    : AnalyzerContext = {

    // 按照是否属于 ScanShareableAnalyzer 类型拆分 Analyzer
    /* Identify shareable analyzers */
    val (shareable, others) = analyzers.partition { _.isInstanceOf[ScanShareableAnalyzer[_, _]] }

    val shareableAnalyzers =
      shareable.map { _.asInstanceOf[ScanShareableAnalyzer[State[_], Metric[_]]] }

    /* Compute aggregation functions of shareable analyzers in a single pass over the data */
    val sharedResults = if (shareableAnalyzers.nonEmpty) {

      val metricsByAnalyzer = try {
        // 获取所有的待计算 column 表达式
        val aggregations = shareableAnalyzers.flatMap { _.aggregationFunctions() }

        // 获取每个 analyzer 对应的 columns 表达式的偏移量
        /* Compute offsets so that the analyzers can correctly pick their results from the row */
        val offsets = shareableAnalyzers.scanLeft(0) { case (current, analyzer) =>
          current + analyzer.aggregationFunctions().length
        }

        // 针对整个数据集进行聚合操作, 并计算各个聚合 column 的值
        // 因为是针对整个数据集进行聚合操作, 因此聚合结果只有一行, 即使用head来获取首个元素即可
        val results = data.agg(aggregations.head, aggregations.tail: _*).collect().head

        shareableAnalyzers.zip(offsets).map { case (analyzer, offset) =>
          // 创建 (Analyzer, Metric) 元组组成的集合
          analyzer ->
            successOrFailureMetricFrom(analyzer, results, offset, aggregateWith, saveStatesTo)
        }

      } catch {
        case error: Exception =>
          shareableAnalyzers.map { analyzer => analyzer -> analyzer.toFailureMetric(error) }
      }

      AnalyzerContext(metricsByAnalyzer.toMap[Analyzer[_, Metric[_]], Metric[_]])
    } else {
      AnalyzerContext.empty
    }

    // 执行 non-shareable 的 analyzer
    /* Run non-shareable analyzers separately */
    val otherMetrics = others
      .map { analyzer => analyzer -> analyzer.calculate(data, aggregateWith, saveStatesTo) }
      .toMap[Analyzer[_, Metric[_]], Metric[_]]

    // 返回 shareableAnalyzer + other analyzer 对应的 (Analyzer,Metric) 结果
    sharedResults ++ AnalyzerContext(otherMetrics)
  }

  /** Compute scan-shareable analyzer metric from aggregation result, mapping generic exceptions
    * to a failure metric */
  private def successOrFailureMetricFrom(
      analyzer: ScanShareableAnalyzer[State[_], Metric[_]],
      aggregationResult: Row,
      offset: Int,
      aggregateWith: Option[StateLoader],
      saveStatesTo: Option[StatePersister])
    : Metric[_] = {

    try {
      // 结合 Repository 中已经计算的 State , 计算最终的 Metric
      analyzer.metricFromAggregationResult(aggregationResult, offset, aggregateWith, saveStatesTo)
    } catch {
      case error: Exception => analyzer.toFailureMetric(error)
    }
  }

  /** Compute frequency based analyzer metric from aggregation result, mapping generic exceptions
    * to a failure metric */
  private def successOrFailureMetricFrom(
      analyzer: ScanShareableFrequencyBasedAnalyzer,
      aggregationResult: Row,
      offset: Int)
    : Metric[_] = {

    try {
      analyzer.fromAggregationResult(aggregationResult, offset)
    } catch {
      case error: Exception => analyzer.toFailureMetric(error)
    }
  }

  /**
    * Compute the metrics from the analyzers configured in the analyis, instead of running
    * directly on data, this computation leverages (and aggregates) existing states which have
    * previously been computed on the data.
    *
    * @param schema schema of the data frame from which the states were computed
    * @param analysis the analysis to compute
    * @param stateLoaders loaders from which we retrieve the states to aggregate
    * @param saveStatesWith persist resulting states for the configured analyzers (optional)
    * @param storageLevelOfGroupedDataForMultiplePasses caching level for grouped data that must be
    *                                                   accessed multiple times (use
    *                                                   StorageLevel.NONE to completely disable
    *                                                   caching)
    * @return AnalyzerContext holding the requested metrics per analyzer
    */
    def runOnAggregatedStates(
      schema: StructType,
      analysis: Analysis,
      stateLoaders: Seq[StateLoader],
      saveStatesWith: Option[StatePersister] = None,
      storageLevelOfGroupedDataForMultiplePasses: StorageLevel = StorageLevel.MEMORY_AND_DISK,
      metricsRepository: Option[MetricsRepository] = None,
      saveOrAppendResultsWithKey: Option[ResultKey] = None                             )
    : AnalyzerContext = {

    if (analysis.analyzers.isEmpty || stateLoaders.isEmpty) {
      return AnalyzerContext.empty
    }

    val analyzers = analysis.analyzers.map { _.asInstanceOf[Analyzer[State[_], Metric[_]]] }

    /* Find all analyzers which violate their preconditions */
    val passedAnalyzers = analyzers
      .filter { analyzer =>
        Preconditions.findFirstFailing(schema, analyzer.preconditions).isEmpty
      }

    val failedAnalyzers = analyzers.diff(passedAnalyzers)

    /* Create the failure metrics from the precondition violations */
    val preconditionFailures = computePreconditionFailureMetrics(failedAnalyzers, schema)

    val aggregatedStates = InMemoryStateProvider()

    /* Aggregate all initial states */
    passedAnalyzers.foreach { analyzer =>
      stateLoaders.foreach { stateLoader =>
        analyzer.aggregateStateTo(aggregatedStates, stateLoader, aggregatedStates)
      }
    }

    /* Identify analyzers which require us to group the data */
    val (groupingAnalyzers, scanningAnalyzers) =
      passedAnalyzers.partition { _.isInstanceOf[GroupingAnalyzer[State[_], Metric[_]]] }

    val nonGroupedResults = scanningAnalyzers
      .map { _.asInstanceOf[Analyzer[State[_], Metric[_]]] }
      .flatMap { analyzer =>
        val metrics = analyzer
          .loadStateAndComputeMetric(aggregatedStates)

        /* Store aggregated state if a 'saveStatesWith' has been provided */
        saveStatesWith.foreach { persister => analyzer.copyStateTo(aggregatedStates, persister) }

        metrics.map { metric => analyzer -> metric }
      }
      .toMap[Analyzer[_, Metric[_]], Metric[_]]


    val groupedResults = if (groupingAnalyzers.isEmpty) {
      AnalyzerContext.empty
    } else {
      groupingAnalyzers
        .map { _.asInstanceOf[GroupingAnalyzer[State[_], Metric[_]]] }
        .groupBy { _.groupingColumns().sorted }
        .map { case (_, analyzersForGrouping) =>

          val state = findStateForParticularGrouping(analyzersForGrouping, aggregatedStates)

          runAnalyzersForParticularGrouping(state, analyzersForGrouping, saveStatesWith,
            storageLevelOfGroupedDataForMultiplePasses)
        }
        .reduce { _ ++ _ }
    }

    val results = preconditionFailures ++ AnalyzerContext(nonGroupedResults) ++ groupedResults

    saveOrAppendResultsIfNecessary(results, metricsRepository, saveOrAppendResultsWithKey)

    results
  }

  /** We only store the grouped dataframe for a particular grouping once; in order to retrieve it
    * for analyzers that require it, we need to test all of them */
  private[this] def findStateForParticularGrouping(
      analyzers: Seq[GroupingAnalyzer[State[_], Metric[_]]], stateLoader: StateLoader)
    : FrequenciesAndNumRows = {

    /* One of the analyzers must have the state persisted */
    val states = analyzers.flatMap { analyzer =>
      stateLoader
        .load[FrequenciesAndNumRows](analyzer.asInstanceOf[Analyzer[FrequenciesAndNumRows, _]])
    }

    require(states.nonEmpty)
    states.head
  }

  /** Efficiently executes the analyzers for a particular grouping,
    * applying scan-sharing where possible */
  private[this] def runAnalyzersForParticularGrouping(
      frequenciesAndNumRows: FrequenciesAndNumRows,
      analyzers: Seq[GroupingAnalyzer[State[_], Metric[_]]],
      saveStatesTo: Option[StatePersister] = None,
      storageLevelOfGroupedDataForMultiplePasses: StorageLevel = StorageLevel.MEMORY_AND_DISK)
    : AnalyzerContext = {

    val numRows = frequenciesAndNumRows.numRows

    /* Identify all shareable analyzers */
    val (shareable, others) =
      analyzers.partition { _.isInstanceOf[ScanShareableFrequencyBasedAnalyzer] }

    /* Potentially cache the grouped data if we need to make several passes,
       controllable via the storage level */
    if (others.nonEmpty) {
      frequenciesAndNumRows.frequencies.persist(storageLevelOfGroupedDataForMultiplePasses)
    }

    val shareableAnalyzers = shareable.map { _.asInstanceOf[ScanShareableFrequencyBasedAnalyzer] }

    val metricsByAnalyzer = if (shareableAnalyzers.nonEmpty) {

      try {
        val aggregations = shareableAnalyzers.flatMap { _.aggregationFunctions(numRows) }
        /* Compute offsets so that the analyzers can correctly pick their results from the row */
        val offsets = shareableAnalyzers.scanLeft(0) { case (current, analyzer) =>
          current + analyzer.aggregationFunctions(numRows).length
        }

        /* Execute aggregation on grouped data */
        val results = frequenciesAndNumRows.frequencies
          .agg(aggregations.head, aggregations.tail: _*)
          .collect()
          .head

        shareableAnalyzers.zip(offsets)
          .map { case (analyzer, offset) =>
            analyzer -> successOrFailureMetricFrom(analyzer, results, offset)
          }
      } catch {
        case error: Exception =>
          shareableAnalyzers
            .map { analyzer => analyzer -> analyzer.toFailureMetric(error) }
      }

    } else {
      Map.empty
    }

    /* Execute remaining analyzers on grouped data */
    val otherMetrics = try {
      others
        .map { _.asInstanceOf[FrequencyBasedAnalyzer] }
        .map { analyzer => analyzer ->
          analyzer.computeMetricFrom(Option(frequenciesAndNumRows))
        }
    } catch {
      case error: Exception =>
        others.map { analyzer => analyzer -> analyzer.toFailureMetric(error) }
    }

    /* Potentially store states */
    saveStatesTo.foreach { _.persist(analyzers.head, frequenciesAndNumRows) }

    frequenciesAndNumRows.frequencies.unpersist()

    AnalyzerContext((metricsByAnalyzer ++ otherMetrics).toMap[Analyzer[_, Metric[_]], Metric[_]])
  }

}

class ReusingNotPossibleResultsMissingException(message: String)
  extends RuntimeException(message)
