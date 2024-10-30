package com.amazon.deequ.analyzers.grouping

import com.amazon.deequ.analyzers.states.GroupSummableRowsState
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, DataFrame}

/**
 * @author TomAndersen
 * @see [[com.amazon.deequ.analyzers.EmptySize]]
 */
case class GroupingEmptySize(
  column: String,
  groupColumns: Seq[String],
  where: Option[String] = None,
  limit: Option[Int] = None
) extends GroupingAggAnalyzer("GroupingEmptySize", groupColumns, where, limit) {

  override def aggregationFunctions(): Seq[Column] = {
    //    (sum(col(COUNT_COL).equalTo(lit(1)).cast(DoubleType)) / numRows) :: Nil
    expr(s"COUNT(IF($column IS NULL OR trim($column) = '', 1, 0))") :: Nil
  }
}
