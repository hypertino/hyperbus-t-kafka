package com.hypertino.hyperbus.transport.kafkatransport

import com.hypertino.hyperbus.transport.api.matchers.RequestMatcher
import com.hypertino.hyperbus.util.{FuzzyIndexItemMetaInfo, FuzzyMatcher}
import com.hypertino.parser.HParser
import com.hypertino.parser.ast.Expression

case class KafkaRoute(requestMatcher: RequestMatcher,
                      kafkaTopic: String,
                      kafkaPartitionKeys: Option[String]) extends FuzzyMatcher {

  val kafkaPartitionKeysExpression: Option[Expression] = kafkaPartitionKeys.map(s ⇒ HParser("s\"" + s + "\"")) // always a string interpolation
  override def indexProperties: Seq[FuzzyIndexItemMetaInfo] = requestMatcher.indexProperties
  override def matches(other: Any): Boolean = requestMatcher.matches(other)
}
