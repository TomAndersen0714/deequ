package com.amazon.deequ.analyzers.metrics

import com.amazon.deequ.metrics.{DoubleMetric, Entity, Metric}

import scala.util.{Failure, Success, Try}

/**
 * @author ericcheng
 */
case class GroupMetric(
  entity: Entity.Value,
  name: String,
  instance: String,
  value: Try[Map[Map[String, _], Map[String, _]]]
) extends Metric[Map[Map[String, _], Map[String, _]]] {

  override def flatten(): Seq[DoubleMetric] = {
    if (value.isSuccess) {
      value.get.flatMap {
          case (groupMap, valueMap) => {
            valueMap.flatMap {
              case (k, v) => {
                DoubleMetric(entity, s"$name-$groupMap-$k", instance, Success(v.toString.toDouble)) +: Nil
              }
            }
          }
        }
        .toSeq
    }
    else {
      Seq(DoubleMetric(entity, s"$name", instance, Failure(value.failed.get)))
    }
  }
}
