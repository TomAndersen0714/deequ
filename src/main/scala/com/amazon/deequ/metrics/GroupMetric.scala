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
