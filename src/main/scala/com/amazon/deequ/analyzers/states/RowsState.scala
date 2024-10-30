package com.amazon.deequ.analyzers.states

import com.amazon.deequ.analyzers.State
import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.apache.spark.sql.{Column, DataFrame}

/**
 * State representing dataframe rows.
 *
 * @author ericcheng
 */
abstract case class RowsState[S <: RowsState[S]](groupedRows: DataFrame) extends State[S] {

  def nullSafeEq(column: String): Column = {
    col(s"this.$column") <=> col(s"other.$column")
  }

  def zeroIfNull(column: String): Column = {
    coalesce(col(column), lit(0))
  }
}
