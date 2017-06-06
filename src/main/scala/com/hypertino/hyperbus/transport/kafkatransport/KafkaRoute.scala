package com.hypertino.hyperbus.transport.kafkatransport

import com.hypertino.hyperbus.transport.api.matchers.RequestMatcher
import com.hypertino.hyperbus.util.{FuzzyIndexItemMetaInfo, FuzzyMatcher}

case class KafkaRoute(requestMatcher: RequestMatcher,
                      kafkaTopic: String,
                      kafkaPartitionKeys: List[String]) extends FuzzyMatcher {

  override def indexProperties: Seq[FuzzyIndexItemMetaInfo] = requestMatcher.indexProperties
  override def matches(other: Any): Boolean = requestMatcher.matches(other)
}
