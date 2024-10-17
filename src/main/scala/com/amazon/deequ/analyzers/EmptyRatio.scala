package com.amazon.deequ.analyzers

import com.amazon.deequ.analyzers.Analyzers.{conditionalCount, conditionalSelection, ifNoNullsIn}
import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isNotNested}
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions.{expr, sum}
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * @author TomAndersen
 */

/** EmptyRatio is the fraction of null and empty values in a column of a DataFrame. */
case class EmptyRatio(column: String, where: Option[String] = None) extends
  StandardScanShareableAnalyzer[NumMatchesAndCount]("EmptyRatio", column) with
  FilterableAnalyzer {

  override def fromAggregationResult(result: Row, offset: Int): Option[NumMatchesAndCount] = {

    ifNoNullsIn(result, offset, howMany = 2) { _ =>
      NumMatchesAndCount(result.getLong(offset), result.getLong(offset + 1))
    }
  }

  override def aggregationFunctions(): Seq[Column] = {
    val filteredColumn: Column = conditionalSelection(column, where)
    val isNotEmptyColumn: Column = expr(s"$filteredColumn IS NULL OR trim($filteredColumn) = ''")
    val filteredSummation: Column = sum(isNotEmptyColumn.cast(IntegerType))

    filteredSummation :: conditionalCount(where) :: Nil
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column) :: isNotNested(column) :: Nil
  }

  override def filterCondition: Option[String] = where
}
