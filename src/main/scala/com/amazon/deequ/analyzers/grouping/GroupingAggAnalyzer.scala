package com.amazon.deequ.analyzers.grouping

import com.amazon.deequ.analyzers.Analyzers.{COUNT_COL, emptyStateException, entityFrom, ifNoNullsIn}
import com.amazon.deequ.analyzers.Preconditions.{atLeastOne, hasColumn, isNotNested}
import com.amazon.deequ.analyzers.metrics.GroupMetric
import com.amazon.deequ.analyzers.runners.MetricCalculationException
import com.amazon.deequ.analyzers.states.{GroupSummableRowsState, RowsState}
import com.amazon.deequ.analyzers.{FilterableAnalyzer, FrequenciesAndNumRows, GroupingAnalyzer, ScanShareableAnalyzer}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions.{col, count, expr, lit}
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success}

/**
 * @author TomAndersen
 * @see [[com.amazon.deequ.analyzers.FrequencyBasedAnalyzer]]
 * @see [[com.amazon.deequ.analyzers.EmptyRatio]]
 * @see [[]]
 */
abstract class GroupingAggAnalyzer(
    name: String,
    groupColumns: Seq[String],
    where: Option[String] = None,
    limit: Option[Int] = None
  )
  extends GroupingAnalyzer[GroupSummableRowsState, GroupMetric]
  with FilterableAnalyzer {

  /** The columns to group the data by */
  override def groupingColumns(): Seq[String] = groupColumns

  /** Dataset filtering */
  override def filterCondition: Option[String] = where

  /** We need at least one grouping column, and all specified columns must exist and not be nested */
  override def preconditions: Seq[StructType => Unit] = {

    // Check if the groupColumns are not empty
    atLeastOne(groupColumns)

    // Check the existence and datatypes of the groupColumns
    groupColumns.map {
      hasColumn
    } ++ groupColumns.map {
      isNotNested
    } ++ super.preconditions
  }

  /** Defines the aggregations to compute on the data */
  def aggregationFunctions(): Seq[Column] = {
    expr(s"COUNT(1)").alias(s"$COUNT_COL") :: Nil
  }

  override def computeStateFrom(data: DataFrame): Option[GroupSummableRowsState] = {

    val aggregations = aggregationFunctions()
    val groupColumnsExpr = groupColumns.map(col)

    val groupedAggData = data
      .transform(filterOptional(filterCondition))
      .groupBy(groupColumnsExpr: _*)
      .agg(aggregations.head, aggregations.tail: _*)

    val groupedAggRows = limit match {
      case Some(limitValue) => groupedAggData.limit(limitValue)
      case _ => groupedAggData
    }

    Some(GroupSummableRowsState(groupedAggRows, groupColumns))
  }


  override def computeMetricFrom(state: Option[GroupSummableRowsState]): GroupMetric = {

    state match {
      case Some(theState) =>
        // todo: 从 state(grouping dataframe) 中获取
        val aggregations = aggregationFunctions()

        // todo: 触发运算 读取 DataFrame, 填充 Metric
        val result = theState.groupedAggRows.agg(aggregations.head, aggregations.tail: _*).collect()
          .head
      case None =>
        toFailureMetric(MetricCalculationException.wrapIfNecessary(emptyStateException(this)))
    }
  }

  override def toFailureMetric(exception: Exception): GroupMetric = {
    GroupMetric(
      entityFrom(groupColumns), name, groupColumns.mkString(","),
      Failure(exception)
    )
  }

  // todo, 修复输入参数类型并支持调用
  //  def toSuccessMetric(value: Double): GroupMetric = {
  //    GroupMetric(
  //      entityFrom(groupColumns), name, groupColumns.mkString(","),
  //      Success(value)
  //    )
  //  }

  private def filterOptional(where: Option[String])(data: DataFrame): DataFrame = {
    where match {
      case Some(condition) => data.filter(condition)
      case _ => data
    }
  }
}

case object GroupingAggAnalyzer {

  /** Compute the aggregation functions of groups in the data once, essentially via a query like
   *
   * SELECT colA, colB, ..., COUNT(*), SUM(), MAX()
   * FROM DATA
   * WHERE colA IS NOT NULL OR colB IS NOT NULL OR ...
   * GROUP BY colA, colB, ...
   */
  def computeStateFrom(
      data: DataFrame,
      groupColumns: Seq[String],
      aggregations: Seq[Column],
      where: Option[String] = None,
      limit: Option[Int] = None
   ): Option[RowsState[GroupSummableRowsState]] = {

    val groupColumnsExpr = groupColumns.map(col)

    val groupedAggData = data
      .transform(filterOptional(where))
      .groupBy(groupColumnsExpr: _*)
      .agg(aggregations.head, aggregations.tail: _*)

    val groupedAggRows = limit match {
      case Some(limitValue) => groupedAggData.limit(limitValue)
      case _ => groupedAggData
    }

    Some(GroupSummableRowsState(groupedAggRows, groupColumns))
  }

  def filterOptional(where: Option[String])(data: DataFrame): DataFrame = {
    where match {
      case Some(condition) => data.filter(condition)
      case _ => data
    }
  }
}
