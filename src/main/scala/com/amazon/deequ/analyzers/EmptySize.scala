/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */
package com.amazon.deequ.analyzers

import com.amazon.deequ.analyzers.Analyzers._
import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isNotNested}
import org.apache.spark.sql.functions.{col, expr, sum, when}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{Column, Row}

/** EmptySize is the number of null and empty values in a DataFrame. */
case class EmptySize(column: String, where: Option[String] = None)
  extends StandardScanShareableAnalyzer[NumMatches]("EmptySize", column)
    with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {
    val selectColumn = col(column)
    val filteredColumn = where.map {
      condition => when(expr(condition), selectColumn)
    }.getOrElse(column)

    val isNotEmptyColumn: Column = expr(s"$filteredColumn IS NULL OR trim($filteredColumn) = ''")
    val filteredSummation: Column = sum(isNotEmptyColumn.cast(IntegerType))

    filteredSummation :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[NumMatches] = {
    ifNoNullsIn(result, offset) { _ =>
      NumMatches(result.getLong(offset))
    }
  }

  override def filterCondition: Option[String] = where
}
