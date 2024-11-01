package com.amazon.deequ.analyzers.grouping

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr

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
    expr(s"COUNT(IF($column IS NULL OR trim($column) = '', 1, 0))") :: Nil
  }
}
