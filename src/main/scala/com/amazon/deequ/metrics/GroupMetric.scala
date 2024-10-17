package com.amazon.deequ.metrics

import scala.util.Try

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
    Nil
  }
}
