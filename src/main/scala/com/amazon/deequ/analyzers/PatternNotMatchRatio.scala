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
import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isString}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{Column, Row}

import scala.util.matching.Regex

case class PatternNotMatchRatio(column: String, pattern: Regex, where: Option[String] = None)
  extends StandardScanShareableAnalyzer[NumMatchesAndCount]("PatternNotMatchRatio", column)
    with FilterableAnalyzer {

  override def fromAggregationResult(result: Row, offset: Int): Option[NumMatchesAndCount] = {
    ifNoNullsIn(result, offset, howMany = 2) { _ =>
      NumMatchesAndCount(result.getLong(offset), result.getLong(offset + 1))
    }
  }

  override def aggregationFunctions(): Seq[Column] = {
    // 若匹配结果为空, 即未匹配上, 则结果为1, 否则结果为0
    val expression = when(regexp_extract(col(column), pattern.toString(), 0).equalTo(lit("")), 1)
      .otherwise(0)

    val summation = sum(conditionalSelection(expression, where).cast(IntegerType))

    summation :: conditionalCount(where) :: Nil
  }

  override def filterCondition: Option[String] = where

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column) :: isString(column) :: Nil
  }
}
