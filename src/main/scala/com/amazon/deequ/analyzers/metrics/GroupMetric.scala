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
package com.amazon.deequ.analyzers.metrics

import com.amazon.deequ.metrics.{DoubleMetric, Entity, Metric}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
 * @author ericcheng
 */
case class GroupMetric(
  entity: Entity.Value,
  name: String,
  instance: String,
  value: Try[Map[String, _]]
) extends Metric[Map[String, _]] {

  override def flatten(): Seq[DoubleMetric] = {
    if (value.isSuccess) {
      val namedValues = flattenValue(value.get)
      namedValues.map {
        case (k, v) => {
          DoubleMetric(entity, s"$name", s"$instance-$k", Success(v))
        }
      }.toSeq
    }
    else {
      Seq(DoubleMetric(entity, s"$name", instance, Failure(value.failed.get)))
    }
  }

  private def flattenValue(value: Map[String, _]): Map[String, Double] = {
    val path = mutable.ArrayBuffer[String]()
    val namedValues = mutable.Map[String, Double]()

    def dfs(treeNodes: Map[String, _]): Unit = {
      treeNodes.foreach {
        case (k, v) => {
          path.append(k)
          if (v.isInstanceOf[scala.collection.Map[_, _]]) {
            dfs(v.asInstanceOf[Map[String, _]])
          }
          else {
            namedValues.update(path.toString(), v.toString.toDouble)
          }
          path.trimEnd(1)
        }
      }
    }

    dfs(value)
    namedValues.toMap
  }
}
