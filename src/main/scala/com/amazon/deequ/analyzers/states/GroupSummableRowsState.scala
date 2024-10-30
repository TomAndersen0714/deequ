package com.amazon.deequ.analyzers.states

import com.amazon.deequ.analyzers.Analyzers.COUNT_COL
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col}

/**
 * 支持分组+可加性聚合指标 State
 *
 * @author ericcheng
 * @see
 */
case class GroupSummableRowsState(groupedAggRows: DataFrame, groupColumns: Seq[String] = Nil)
  extends RowsState[GroupSummableRowsState](groupedAggRows) {

  /** Add up frequencies via an outer-join */
  override def sum(other: GroupSummableRowsState): GroupSummableRowsState = {

    val aggColumns: Seq[String] = groupedAggRows.schema.fields.map {
      _.name
    }.filterNot {
      groupColumns.contains(_)
    }

    // Null-safe join condition over equality on grouping columns
    val joinCondition = groupColumns.tail
      .foldLeft(nullSafeEq(groupColumns.head)) {
        case (expr, column) => expr.and(nullSafeEq(column))
      }

    val columnsAfterJoin =
      groupColumns.map {
        // Coalesce the values of the group columns
        column => coalesce(col(s"this.$column"), col(s"other.$column")).as(column)
      } ++ aggColumns.map {
        // Sum up the values of the aggregated columns
        column => (zeroIfNull(s"this.$column") + zeroIfNull(s"other.$column")).as(column)
      }

    // Full outer join
    val groupCountMerge = groupedAggRows.alias("this")
      .join(other.groupedRows.alias("other"), joinCondition, "outer")
      .select(columnsAfterJoin: _*)

    GroupSummableRowsState(groupCountMerge)
  }
}
