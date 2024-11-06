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
) extends GroupingAggAnalyzer("GroupingEmptySize", column, groupColumns, where, limit) {

  override def aggregationFunction(): Column = {
    expr(s"SUM(IF($column IS NULL OR trim($column) = '', 1, 0))").alias("GroupingEmptySize")
  }
}
