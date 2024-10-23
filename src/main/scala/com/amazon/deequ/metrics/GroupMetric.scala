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

package com.amazon.deequ.metrics

import scala.util.{Failure, Try}

/**
 * @author TomAndersen
 */
case class GroupMetric(
  entity: Entity.Value,
  name: String,
  instance: String,
  value: Try[Map[String, Metric[_]]]
) extends Metric[Map[String, Metric[_]]] {

  override def flatten(): Seq[DoubleMetric] = {
    value
      .map {
        metrics: Map[String, Metric[_]] => {
          metrics.values.toSeq.flatMap(
            metric => metric.flatten()
          )
        }
      }
      .recover {
        case e: Exception => Seq(DoubleMetric(entity, s"$name", instance, Failure(e)))
      }
      .get
  }
}
