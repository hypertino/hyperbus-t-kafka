package com.hypertino.hyperbus.transport

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.hypertino.hyperbus.model.{RequestBase, ResponseBase}
import com.hypertino.hyperbus.serialization.ResponseBaseDeserializer
import com.hypertino.hyperbus.transport.api._
import com.hypertino.hyperbus.transport.kafkatransport.{ConfigLoader, KafkaPartitionKeyIsNotDefined, KafkaRoute}
import com.hypertino.hyperbus.util.ConfigUtils._
import com.hypertino.hyperbus.util.{FuzzyIndex, SchedulerInjector}
import com.typesafe.config.Config
import monix.eval.Task
import monix.execution.{Cancelable, Scheduler}
import monix.kafka.{KafkaProducer, KafkaProducerConfig}
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory
import scaldi.Injector

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

class KafkaClientTransport(producerConfig: KafkaProducerConfig,
                           routes: List[KafkaRoute],
                           encoding: String = "UTF-8")
                          (implicit val scheduler: Scheduler) extends ClientTransport {

  def this(config: Config, injector: Injector) = this(
    producerConfig = ConfigLoader.loadProducerConfig(config.getConfig("producer")),
    routes = ConfigLoader.loadRoutes(config.getConfigList("routes")),
    encoding = config.getOptionString("encoding").getOrElse("UTF-8")
  )(SchedulerInjector(config.getOptionString("scheduler"))(injector))

  protected[this] val routesIndex = new FuzzyIndex[KafkaRoute](routes: _*)
  protected[this] val log = LoggerFactory.getLogger(this.getClass)
  protected[this] val producer = KafkaProducer[String, String](producerConfig, Scheduler.io("kafka-client"))

  override def ask(message: RequestBase, responseDeserializer: ResponseBaseDeserializer): Task[ResponseBase] = ???

  override def publish(message: RequestBase): Task[PublishResult] = {
    routesIndex.lookupAll(message).headOption map (publishToRoute(_, message)) getOrElse {
      throw new NoTransportRouteException(s"Kafka producer (client): ${message.headers.hrl}")
    }
  }

  override def shutdown(duration: FiniteDuration): Task[Boolean] = {
    producer.close().timeout(duration).map { _ ⇒
      true
    } onErrorRecover {
      case e: Throwable ⇒
        log.error("Can't close kafka producer", e)
        false
    }
  }

  private def publishToRoute(route: KafkaRoute, message: RequestBase): Task[PublishResult] = {
    val messageString = message.serializeToString

    val record: ProducerRecord[String, String] =
      if (route.kafkaPartitionKeys.isEmpty) {
        // no partition key
        new ProducerRecord(route.kafkaTopic, messageString)
      }
      else {
        val recordKey = route.kafkaPartitionKeys.map { key: String ⇒ // todo: check partition key logic
          message.headers.hrl.query.toMap.getOrElse(key,
            throw new KafkaPartitionKeyIsNotDefined(s"Argument $key is not defined for ${message.headers.hrl}")
          )
        }.foldLeft("")(_ + "," + _.toString.replace("\\", "\\\\").replace(",", "\\,"))

        new ProducerRecord(route.kafkaTopic, recordKey.substring(1), messageString)
      }

    producer.send(record).map {
      case Some(recordMetadata) ⇒
        if (log.isTraceEnabled) {
          log.trace(s"Message $message is published to ${recordMetadata.topic()} ${if (record.key() != null) "/" + record.key}: ${recordMetadata.partition()}/${recordMetadata.offset()}")
        }
        new PublishResult {
          def sent = Some(true)
          def offset = Some(s"${recordMetadata.partition()}/${recordMetadata.offset()}}")
          override def toString = s"PublishResult(sent=$sent,offset=$offset)"
        }
      case None ⇒ // todo: canceled?
        new PublishResult {
          def sent = Some(false)
          def offset = None
          override def toString = s"PublishResult(sent=$sent,offset=$offset)"
        }
    } doOnFinish {
      case Some(error) ⇒
        Task.now {
          log.error(s"Can't send to kafka. ${route.kafkaTopic} ${if (record.key() != null) "/" + record.key} : $message", error)
        }
      case None ⇒
        Task.unit
    }
  }
}
