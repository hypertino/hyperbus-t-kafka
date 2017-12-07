package com.hypertino.hyperbus.transport.kafkatransport

import java.util.Properties

import com.hypertino.hyperbus.transport.api.matchers.RequestMatcher
import com.typesafe.config.{Config, ConfigFactory}
import monix.kafka.{KafkaConsumerConfig, KafkaProducerConfig}

object ConfigLoader {
  import scala.collection.JavaConverters._

  def loadRoutes(routesConfigList: java.util.List[_ <: Config]): List[KafkaRoute] = {
    import com.hypertino.binders.config.ConfigBinders._

    routesConfigList.asScala.map { config ⇒
      val kafkaTopic = config.read[KafkaTopicPojo]("kafka")
      val matcher = if (config.hasPath("match"))
        RequestMatcher(config.getValue("match"))
      else
        RequestMatcher.any
      KafkaRoute(matcher, kafkaTopic.topic, kafkaTopic.partitionKeys)
    }.toList
  }

  private def loadProperties(config: Config, defaultProperties: Map[String, String]) = {
    val properties = new Properties()
    config.entrySet().asScala.map { entry ⇒
      properties.setProperty(entry.getKey, entry.getValue.unwrapped().toString)
    }
    defaultProperties.foreach(kv ⇒
      if (properties.getProperty(kv._1) == null)
        properties.setProperty(kv._1, kv._2)
    )
    properties
  }

  def loadProducerConfig(config: Config): KafkaProducerConfig = KafkaProducerConfig(config withFallback defaultProducerConf)

  def loadConsumerConfig(config: Config): KafkaConsumerConfig = KafkaConsumerConfig(config withFallback defaultConsumerConf)

  lazy private val defaultConsumerConf: Config =
    ConfigFactory.load("com/hypertino/hyperbus/transport/kafkatransport/default-consumer.conf").getConfig("kafka")

  lazy private val defaultProducerConf: Config =
    ConfigFactory.load("com/hypertino/hyperbus/transport/kafkatransport/default-producer.conf").getConfig("kafka")
}

private[kafkatransport] case class KafkaTopicPojo(topic: String, partitionKeys: Option[String])