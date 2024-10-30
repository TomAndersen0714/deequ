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
  value: Try[Map[Map[String, String], Double]]
) extends Metric[Map[Map[String, String], Double]] {

  override def flatten(): Seq[DoubleMetric] = {
    if (value.isSuccess) {
      value.get.map {
          case (key, correspondingValue) =>
            DoubleMetric(entity, s"$name-$key", instance, Success(correspondingValue))
        }
        .toSeq
    }
    else {
      Seq(DoubleMetric(entity, s"$name", instance, Failure(value.failed.get)))
    }
  }
}
