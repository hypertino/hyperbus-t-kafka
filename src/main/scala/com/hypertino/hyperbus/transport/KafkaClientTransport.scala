package com.hypertino.hyperbus.transport

import com.hypertino.binders.value.Obj
import com.hypertino.hyperbus.model.{RequestBase, ResponseBase}
import com.hypertino.hyperbus.serialization.ResponseBaseDeserializer
import com.hypertino.hyperbus.transport.api._
import com.hypertino.hyperbus.transport.kafkatransport.{ConfigLoader, KafkaPartitionKeyIsNotDefined, KafkaRoute}
import com.hypertino.hyperbus.util.ConfigUtils._
import com.hypertino.hyperbus.util.{FuzzyIndex, SchedulerInjector}
import com.hypertino.parser.HEval
import com.typesafe.config.Config
import monix.eval.Task
import monix.execution.{Cancelable, Scheduler}
import monix.kafka.{KafkaProducer, KafkaProducerConfig}
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory
import scaldi.Injector

import scala.concurrent.duration.FiniteDuration

class KafkaClientTransport(producerConfig: KafkaProducerConfig,
                           routes: List[KafkaRoute],
                           defaultTopic: Option[String],
                           failedKeysTopic: Option[String],
                           encoding: String = "UTF-8")
                          (implicit val scheduler: Scheduler) extends ClientTransport {

  def this(config: Config, injector: Injector) = this(
    producerConfig = ConfigLoader.loadProducerConfig(config.getConfig("producer")),
    routes = ConfigLoader.loadRoutes(config.getConfigList("routes")),
    defaultTopic = config.getOptionString("default-topic"),
    failedKeysTopic = config.getOptionString("failed-keys-topic"),
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

  private def publishToRoute(route: KafkaRoute, message: RequestBase): Task[PublishResult] = Task.defer {
    val messageString = message.serializeToString
    val record = route.kafkaPartitionKeysExpression.map { e ⇒
      try {
        val context = Obj.from(
          "headers" → Obj(message.headers),
          "location" → message.headers.hrl.location,
          "query" → message.headers.hrl.query,
          "method" → message.headers.method
        )
        val key = new HEval(context)
          .eval(e)
          .toString()
        new ProducerRecord[String,String](route.kafkaTopic, key, messageString)
      } catch {
        case t: Throwable ⇒
          if (failedKeysTopic.isDefined) {
            new ProducerRecord[String,String](failedKeysTopic.get, messageString)
          }
          else {
            throw t
          }
      }
    } getOrElse {
      new ProducerRecord[String, String](route.kafkaTopic, messageString)
    }

    producer.send(record).map {
      case Some(recordMetadata) ⇒
        if (log.isTraceEnabled) {
          log.trace(s"Message $message is published to ${recordMetadata.topic()} ${if (record.key() != null) "/" + record.key}: ${recordMetadata.partition()}/${recordMetadata.offset()}")
        }
        KafkaPublishResult(
          committed=Some(recordMetadata.offset()>0),
          kafkaOffset=recordMetadata.offset(),
          partition=recordMetadata.partition(),
          topic=recordMetadata.topic()
        )
      case None ⇒ // todo: canceled?
        PublishResult.empty
    }
  }
}
