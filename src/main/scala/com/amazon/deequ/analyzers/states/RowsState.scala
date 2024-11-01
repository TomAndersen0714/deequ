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

import com.amazon.deequ.analyzers.State
import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.apache.spark.sql.{Column, DataFrame}

/**
 * State representing dataframe rows.
 *
 * @author ericcheng
 */
abstract class RowsState[S <: RowsState[S]](groupedRows: DataFrame) extends State[S] {

  def nullSafeEq(column: String): Column = {
    col(s"this.$column") <=> col(s"other.$column")
  }

  def zeroIfNull(column: String): Column = {
    coalesce(col(column), lit(0))
  }
}
