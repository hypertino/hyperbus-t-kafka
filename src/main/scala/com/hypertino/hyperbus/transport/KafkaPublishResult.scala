package com.hypertino.hyperbus.transport

import com.hypertino.hyperbus.transport.api.PublishResult

case class KafkaPublishResult(committed: Option[Boolean],
                              kafkaOffset: Long,
                              partition: Int,
                              topic: String
                             ) extends PublishResult {
  override def offset = Some(s"$partition@$kafkaOffset")
}
