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

package com.amazon.deequ.analyzers.states

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col}

/**
 * 支持分组+可加性指标 State
 *
 * @author ericcheng
 * @see [[com.amazon.deequ.analyzers.FrequenciesAndNumRows]]
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
      .join(other.groupedAggRows.alias("other"), joinCondition, "outer")
      .select(columnsAfterJoin: _*)

    GroupSummableRowsState(groupCountMerge)
  }
}
