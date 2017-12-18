package com.hypertino.hyperbus.transport.kafkatransport

import java.io.{Reader, StringReader}

import com.hypertino.binders.value.Obj
import com.hypertino.hyperbus.model.{DynamicRequest, EmptyBody, Headers, RequestBase, RequestHeaders}
import com.hypertino.hyperbus.serialization.{MessageReader, RequestBaseDeserializer}
import com.hypertino.hyperbus.transport.api.matchers.RequestMatcher
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Ack.{Continue, Stop}
import monix.execution.atomic.AtomicAny
import monix.execution.{Ack, Cancelable, Scheduler}
import monix.kafka.{KafkaConsumerConfig, KafkaConsumerObservable}
import monix.reactive.observers.Subscriber
import monix.reactive.{Observable}
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

private[transport] class TopicSubscription(
                                            consumerConfig: KafkaConsumerConfig,
                                            encoding: String,
                                            route: KafkaRoute,
                                            lock: Object,
                                            releaseFun: () ⇒ Unit,
                                            implicit val scheduler: Scheduler
                                          ) extends Subscriber[Option[(RequestBase, String)]] with StrictLogging {
  if (route.kafkaPartitionKeys.nonEmpty) {
    //throw new IllegalArgumentException("Partitioning for server transport isn't supported yet")
    logger.error(s"Partitioning for server transport isn't supported yet. Partition keys are ignored: ${route.kafkaPartitionKeys}")
  }

  private var cancelable = Cancelable.empty
  private val subscribersMapRef = AtomicAny(Map.empty[RequestMatcher, List[(Subscriber[RequestBase], RequestBaseDeserializer)]])
  private lazy val kafkaConsumerObservable = KafkaConsumerObservable[String, String](consumerConfig, List(route.kafkaTopic))
    .executeOn(Scheduler.io("kafka-server"))
    .map(kafkaRecordToHeadersAndBody)
    .doOnTerminate { r ⇒
      logger.info(s"Consumer on $route/$this is terminated with $r")
    }

  def connect(): Unit = {
    cancelable = kafkaConsumerObservable.subscribe(this)
  }

  def close(duration: FiniteDuration): Task[Unit] = {
    Task.eval {
      lock.synchronized {
        cancelable.cancel()
        subscribersMapRef.get.foreach(_._2.foreach(_._1.onComplete()))
        subscribersMapRef.set(Map.empty)
      }
    }
  }

  def add(matcher: RequestMatcher, inputDeserializer: RequestBaseDeserializer): Observable[RequestBase] = {
    new Observable[RequestBase] {
      override def unsafeSubscribeFn(subscriber: Subscriber[RequestBase]): Cancelable = {
        lock.synchronized {
          logger.debug(s"Adding subscription: $matcher")
          val m = subscribersMapRef.get
          val l = m.get(matcher) match {
            case Some(existing) ⇒
              existing :+ (subscriber, inputDeserializer)

            case None ⇒
              List((subscriber, inputDeserializer))
          }
          subscribersMapRef.set(m + (matcher -> l))
        }

        new Cancelable {
          override def cancel(): Unit = {
            cancelSubscriber(matcher, subscriber)
          }
        }
      }
    }
  }

  override def onNext(elem: Option[(RequestBase, String)]): Future[Ack] = elem match {
    case Some(e) ⇒
      val m = subscribersMapRef.get
      m.keys.find(_.matches(e._1)).map { r: RequestMatcher ⇒
        val l = m(r)
        val (subscriber, inputDeserializer) = getRandom(l).get
        val msg: Option[RequestBase] = try {
          val reader = new StringReader(e._2)
          Some(inputDeserializer(reader, e._1.headers.underlying))
        }
        catch {
          case t: Throwable ⇒
            logger.error(s"Can't deserialize record: ${e._1.headers.underlying}: ${e._2}", t)
            None
        }

        val ack: Future[Ack] = msg match {
          case Some(finalMsg) ⇒
            subscriber.onNext(finalMsg).map {
              case Continue ⇒ Continue
              case Stop ⇒
                lock.synchronized {
                  if (cancelSubscriber(r, subscriber)) {
                    Stop
                  }
                  else {
                    Continue
                  }
                }
            }

          case None ⇒
            Continue
        }
        ack
      } getOrElse {
        logger.error(s"Message didn't matched any subscription: ${e._1.headers.underlying}: ${e._2}")
        Continue
      }

    case None ⇒
      Continue
  }

  override def onError(ex: Throwable): Unit = {
    lock.synchronized {
      subscribersMapRef.get.foreach(_._2.foreach(_._1.onError(ex)))
      subscribersMapRef.set(Map.empty)
      releaseFun()
    }
  }

  override def onComplete(): Unit = {
    lock.synchronized {
      subscribersMapRef.get.foreach(_._2.foreach(_._1.onComplete()))
      subscribersMapRef.set(Map.empty)
      releaseFun()
    }
  }

  private def cancelSubscriber(matcher: RequestMatcher, subscriber: Subscriber[RequestBase]): Boolean = {
    lock.synchronized {
      val m = subscribersMapRef.get
      val l = m.get(matcher) match {
        case Some(existing) ⇒
          existing.filterNot(_._1 == subscriber)

        case None ⇒
          List.empty
      }
      val m2 = if (l.isEmpty) {
        m - matcher
      }
      else {
        m + (matcher → l)
      }
      subscribersMapRef.set(m2)
      if (m2.isEmpty) {
        releaseFun()
        true
      }
      else {
        false
      }
    }
  }

  private def kafkaRecordToHeadersAndBody(elem: ConsumerRecord[String, String]): Option[(RequestBase, String)] = {
    logger.trace(s"kafka~> key ${elem.key()}, ${elem.topic()}@${elem.partition}#${elem.offset()}: ${elem.value()}")
    var body: String = null
    try {
      val requestWithHeaders = MessageReader.fromString[RequestBase](elem.value(), (r: Reader, h: Headers) ⇒ {
        body = readBody(r)
        DynamicRequest(EmptyBody, RequestHeaders(h))
      })

      Some(requestWithHeaders, body)
    } catch {
      case t: Throwable ⇒
        logger.error(s"Can't deserialize record: ${elem.key()}, ${elem.topic()}@${elem.partition}#${elem.offset()}: ${elem.value()}", t)
        None
    }
  }

  private def readBody(reader: Reader): String = {
    val bufferSize = 1024
    val buffer = new Array[Char](bufferSize)
    val out = new StringBuilder()
    var rsz = 0
    do {
      rsz = reader.read(buffer, 0, buffer.length)
      if (rsz > 0) {
        out.appendAll(buffer, 0, rsz)
      }
    } while (rsz > 0)
    out.toString()
  }

  private val random = new Random()

  def getRandom[T](seq: Seq[T]): Option[T] = {
    val size = seq.size
    if (size > 1)
      Some(seq(random.nextInt(size)))
    else
      seq.headOption
  }
}