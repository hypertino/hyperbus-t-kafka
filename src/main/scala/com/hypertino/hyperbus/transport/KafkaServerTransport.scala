package com.hypertino.hyperbus.transport

import com.hypertino.hyperbus.model.{RequestBase, RequestBaseCanFuzzyMatchable}
import com.hypertino.hyperbus.serialization._
import com.hypertino.hyperbus.transport.api._
import com.hypertino.hyperbus.transport.api.matchers.RequestMatcher
import com.hypertino.hyperbus.transport.kafkatransport._
import com.hypertino.hyperbus.util.ConfigUtils._
import com.hypertino.hyperbus.util.{FuzzyIndex, SchedulerInjector}
import com.typesafe.config.Config
import monix.eval.Task
import monix.execution.Scheduler
import monix.kafka.KafkaConsumerConfig
import monix.reactive.Observable
import org.slf4j.LoggerFactory
import scaldi.Injector

import scala.collection.mutable
import scala.concurrent.duration._

class KafkaServerTransport(
                            consumerConfig: KafkaConsumerConfig,
                            routes: List[KafkaRoute],
                            encoding: String = "UTF-8")
                          (implicit val scheduler: Scheduler) extends ServerTransport {
  def this(config: Config, injector: Injector) = this(
    consumerConfig = ConfigLoader.loadConsumerConfig(config.getConfig("consumer")),
    routes = ConfigLoader.loadRoutes(config.getConfigList("routes")),
    encoding = config.getOptionString("encoding") getOrElse "UTF-8"
  )(SchedulerInjector(config.getOptionString("scheduler"))(injector))


  protected[this] val routesIndex = new FuzzyIndex[KafkaRoute](routes: _*)
  protected[this] val subscriptions = mutable.Map[TopicSubscriptionKey, TopicSubscription]()
  protected[this] val lock = new Object
  protected[this] val log = LoggerFactory.getLogger(this.getClass)

  override def commands[REQ <: RequestBase](matcher: RequestMatcher, inputDeserializer: RequestDeserializer[REQ]): Observable[CommandEvent[REQ]] = ???

  override def events[REQ <: RequestBase](matcher: RequestMatcher, groupName: String, inputDeserializer: RequestDeserializer[REQ]): Observable[REQ] = {
    routesIndex.lookupAll(matcher).headOption match {
      case Some(route) ⇒
        val key = TopicSubscriptionKey(route.kafkaTopic, route.kafkaPartitionKeys, groupName)
        lock.synchronized {
          val subscription = subscriptions.getOrElseUpdate(key, {
            new TopicSubscription(consumerConfig.copy(groupId=groupName), encoding, route, lock, () ⇒ removeSubscription(key), scheduler)
          })

          subscription.add(matcher, inputDeserializer).asInstanceOf[Observable[REQ]]
        }

      case None ⇒ Observable.raiseError(new NoTransportRouteException(s"Kafka consumer (server). matcher: $matcher"))
    }
  }

  override def shutdown(duration: FiniteDuration): Task[Boolean] = {
    Task
      .gatherUnordered {
        subscriptions.map(_._2.close(duration))
      }
      .timeout(duration)
      .map {
        _.forall(_ == true)
      }
  }

  private def removeSubscription(key: TopicSubscriptionKey) = {
    subscriptions.remove(key)
  }
}






