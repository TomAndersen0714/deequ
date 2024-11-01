package com.amazon.deequ.analyzers.grouping

import com.amazon.deequ.analyzers.Analyzers.{COUNT_COL, emptyStateException, entityFrom}
import com.amazon.deequ.analyzers.Preconditions.{atLeastOne, hasColumn, isNotNested}
import com.amazon.deequ.analyzers.metrics.GroupMetric
import com.amazon.deequ.analyzers.runners.{AnalyzerContext, MetricCalculationException}
import com.amazon.deequ.analyzers.states.GroupSummableRowsState
import com.amazon.deequ.analyzers.{FilterableAnalyzer, GroupingAnalyzer}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Row}

import scala.util.{Failure, Success}

/**
 * @author TomAndersen
 * @see [[com.amazon.deequ.analyzers.FrequencyBasedAnalyzer]]
 * @see [[com.amazon.deequ.analyzers.EmptyRatio]]
 */
abstract class GroupingAggAnalyzer(
                                    name: String,
                                    groupColumns: Seq[String],
                                    where: Option[String] = None,
                                    limit: Option[Int] = Some(100)
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
    Some(GroupingAggAnalyzer.computeStateFrom(data, groupColumns, aggregationFunctions(), where, limit))
  }


  override def computeMetricFrom(state: Option[GroupSummableRowsState]): GroupMetric = {
    state match {
      case Some(theState) =>
        val metricValue = theState.groupedAggRows.select(
          groupingColumns().map(col) ++ aggregationFunctions(): _*
        )

        // action the dataframe using collect operation
        val metricSimpleValue = metricValue.collect().map {
          row: Row => {
            // get all formatted expressions of column and corresponding values
            val groupMap = row.getValuesMap[String](groupColumns)
            val aggMap = row.getValuesMap[String](
              aggregationFunctions().map(
                column => {
                  column.expr match {
                    // if aggregation function column has alias, use alias as key
                    case alias: Alias =>
                      alias.name
                    // else use formatted expression as key
                    case _ => expr(column.toString()).toString()
                  }
                }
              )
            )
            (groupMap, aggMap)
          }
        }.toMap
        GroupMetric(
          entityFrom(groupColumns), name, groupColumns.mkString(","),
          Success(metricSimpleValue.asInstanceOf[Map[Map[String, _], Map[String, _]]])
        )
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

  // todo, toSuccessMetric
}

/**
 * @see [[com.amazon.deequ.analyzers.FrequencyBasedAnalyzer$]]
 */
object GroupingAggAnalyzer {

  /** Compute the aggregation functions of groups in the data once, essentially via a query like
   *
   * SELECT colA, colB, ..., COUNT(1), SUM(col1), MAX(col2)
   * FROM DATA
   * WHERE colA IS NOT NULL OR colB IS NOT NULL OR ...
   * GROUP BY colA, colB, ...
   */
  def computeStateFrom(
                        data: DataFrame,
                        groupColumns: Seq[String],
                        aggregations: Seq[Column],
                        where: Option[String] = None,
                        limit: Option[Int] = Some(100)
                      ): GroupSummableRowsState = {

    val groupColumnsExpr = groupColumns.map(col)

    val groupedAggData = data
      .transform(filterOptional(where))
      .groupBy(groupColumnsExpr: _*)
      .agg(aggregations.head, aggregations.tail: _*)

    val groupedAggRows = limit match {
      case Some(limitValue) => groupedAggData.limit(limitValue)
      case _ => groupedAggData
    }

    GroupSummableRowsState(groupedAggRows, groupColumns)
  }

  def filterOptional(where: Option[String])(data: DataFrame): DataFrame = {
    where match {
      case Some(condition) => data.filter(condition)
      case _ => data
    }
  }

  def analyzerContextFromState(state: GroupSummableRowsState, analyzers: Seq[GroupingAggAnalyzer])
  : AnalyzerContext = {

    AnalyzerContext(
      analyzers.map {
        analyzer => analyzer -> analyzer.computeMetricFrom(Some(state))
      }.toMap
    )
  }
}
