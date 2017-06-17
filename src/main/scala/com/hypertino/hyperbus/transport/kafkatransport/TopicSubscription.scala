package com.hypertino.hyperbus.transport.kafkatransport

import java.io.Reader

import com.hypertino.binders.value.Obj
import com.hypertino.hyperbus.model.{DynamicRequest, EmptyBody, HeadersMap, RequestBase, RequestHeaders}
import com.hypertino.hyperbus.serialization.{MessageReader, RequestBaseDeserializer}
import com.hypertino.hyperbus.transport.api.matchers.RequestMatcher
import monix.eval.Task
import monix.execution.Ack.Continue
import monix.execution.{Ack, Cancelable, Scheduler}
import monix.execution.atomic.{AtomicAny, AtomicInt}
import monix.kafka.{KafkaConsumerConfig, KafkaConsumerObservable}
import monix.reactive.observers.Subscriber
import monix.reactive.{Observable, Observer, OverflowStrategy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

private[transport] class TopicSubscription(
                                            consumerConfig: KafkaConsumerConfig,
                                            encoding: String,
                                            route: KafkaRoute,
                                            lock: Object,
                                            releaseFun: () ⇒ Unit,
                                            implicit val scheduler: Scheduler
                                          ) extends Observer[ConsumerRecord[String, String]] {
  if (route.kafkaPartitionKeys.nonEmpty) {
    throw new IllegalArgumentException("KafkaServer: Partitioning isn't supported yet")
  }

  private val log = LoggerFactory.getLogger(this.getClass)
  private var cancelable = Cancelable.empty
  private val subscribersRef = AtomicAny(List.empty[(Subscriber.Sync[RequestBase], RequestMatcher, RequestBaseDeserializer)])
  private lazy val observable = KafkaConsumerObservable[String,String](consumerConfig, List(route.kafkaTopic)).executeOn(Scheduler.io("kafka-server"))
  private val MAX_BUFFER_SIZE = 4096 // todo: make configurable?

  def close(duration: FiniteDuration): Task[Unit] = {
    Task.eval {
      lock.synchronized {
        cancelable.cancel()
        subscribersRef.get.foreach(_._1.onComplete())
        subscribersRef.set(List.empty)
      }
    }
  }

  def add(matcher: RequestMatcher, inputDeserializer: RequestBaseDeserializer): Observable[RequestBase] = {
    Observable.create(OverflowStrategy.Fail(MAX_BUFFER_SIZE)) { subscriber: Subscriber.Sync[RequestBase] ⇒
      lock.synchronized {
        val isFirst = subscribersRef.get.isEmpty
        subscribersRef.set(subscribersRef.get :+ (subscriber, matcher, inputDeserializer))
        if (isFirst) {
          cancelable = observable.subscribe(this)
        }
      }

      new Cancelable {
        override def cancel(): Unit = {
          lock.synchronized {
            subscribersRef.set(subscribersRef.get.filterNot(_ == subscriber))
            if (subscribersRef.get.isEmpty) {
              cancelable.cancel()
              releaseFun()
            }
          }
        }
      }
    }
  }

  // todo: handle deserialization/onNext exceptions
  override def onNext(elem: ConsumerRecord[String, String]): Future[Ack] = {
    MessageReader.from[RequestBase](elem.value(), (reader: Reader, headersMap: HeadersMap) ⇒ {
      reader.mark(0)
      implicit val fakeRequest: RequestBase = DynamicRequest(EmptyBody, RequestHeaders(headersMap))

      val (subscriber, matcher, inputDeserializer) = getRandom(subscribersRef.get).get
      if (matcher.matchMessage(fakeRequest)) {
        val msg = inputDeserializer(reader, headersMap)
        subscriber.onNext(msg)
        reader.reset()
      }
      fakeRequest
    })
    Continue
  }

  override def onError(ex: Throwable): Unit = {
    log.error(s"Kafka consumer on $route failed", ex)
    subscribersRef.get.foreach(_._1.onError(ex))
    lock.synchronized {
      releaseFun()
    }
  }

  override def onComplete(): Unit = {
    subscribersRef.get.foreach(_._1.onComplete())
    lock.synchronized {
      releaseFun()
    }
  }

  private val random = new Random()
  private def getRandom[T](seq: Seq[T]): Option[T] = {
    val size = seq.size
    if (size > 1)
      Some(seq(random.nextInt(size)))
    else
      seq.headOption
  }
}



/*

  protected[this] val log = LoggerFactory.getLogger(this.getClass)
  protected[this] val subscriptionCounter = new AtomicLong(1)
  protected[this] val underlyingSubscriptions = TrieMap[Long, UnderlyingSubscription[REQ]]()
  protected[this] val subscriptionsByRequest = TrieMap[RequestMatcher, Vector[UnderlyingSubscription[REQ]]]()
  protected[this] val consumerId = this.hashCode().toHexString + "@" + groupName
  protected val randomGen = new Random()



  val consumer = {
    val props = consumerProperties.clone().asInstanceOf[Properties]
    val groupId = props.getProperty("group.id")
    val newGroupId = if (groupId != null) {
      groupId + "." + groupName
    }
    else {
      groupName
    }
    props.setProperty("group.id", newGroupId)
    Consumer.create(new ConsumerConfig(props))
  }
  @volatile var threadPool: ExecutorService = null

  def run(): Unit = {
    threadPool = Executors.newFixedThreadPool(threadCount)
    val consumerMap = consumer.createMessageStreams(Map(route.kafkaTopic → threadCount))
    val streams = consumerMap(route.kafkaTopic)

    streams.map { stream ⇒
      threadPool.submit(new Runnable {
        override def run(): Unit = consumeStream(stream)
      })
    }
  }

  def stop(duration: FiniteDuration): Unit = {
    consumer.commitOffsets
    consumer.shutdown()
    val t = threadPool
    if (t != null) {
      t.shutdown()
      if (duration.toMillis > 0) {
        try {
          t.awaitTermination(duration.toMillis, TimeUnit.MILLISECONDS)
        }
        catch {
          case t: InterruptedException ⇒ // .. do nothing
        }
      }
      threadPool = null
    }
  }

  def addUnderlying(subscription: UnderlyingSubscription[REQ]): Long = {
    val nextId = subscriptionCounter.incrementAndGet()
    this.underlyingSubscriptions.put(nextId, subscription)
    this.subscriptionsByRequest.get(subscription.requestMatcher) match {
      case Some(vector) ⇒
        subscriptionsByRequest.put(subscription.requestMatcher, vector :+ subscription)
      case None ⇒
        subscriptionsByRequest.put(subscription.requestMatcher, Vector(subscription))
    }
    log.info(s"+1 handler on consumer #$consumerId on topic ${route.kafkaTopic} -> ${subscription.requestMatcher}")
    nextId
  }

  def removeUnderlying(id: Long): Boolean = {
    this.underlyingSubscriptions.remove(id).map { subscription ⇒
      this.subscriptionsByRequest.get(subscription.requestMatcher) match {
        case Some(vector) ⇒
          val newVector = vector.filterNot(_ == subscription)
          if (newVector.isEmpty) {
            this.subscriptionsByRequest.remove(subscription.requestMatcher)
          } else {
            subscriptionsByRequest.put(subscription.requestMatcher, newVector)
          }
        case None ⇒
          // in theory this shouldn't happen
      }
    }
    this.underlyingSubscriptions.isEmpty
  }

  protected def consumeMessage(next: kafka.message.MessageAndMetadata[Array[Byte], Array[Byte]]): Unit = {
    val message = next.message()
    lazy val messageString = new String(message, encoding)
    if (log.isTraceEnabled) {
      log.trace(s"Consumer $consumerId got message from kafka ${next.topic}/${next.partition}${next.offset}: $messageString")
    }
    try {
      var atLeastOneHandler = false

      subscriptionsByRequest.values.foreach { underlyingVector ⇒
        val underlying = if(underlyingVector.size > 1) {
          underlyingVector(randomGen.nextInt(underlyingVector.size))
        } else {
          underlyingVector.head
        }

        val inputBytes = new ByteArrayInputStream(message)
        val input = MessageDeserializer.deserializeRequestWith(inputBytes)(underlying.inputDeserializer)
        if (underlying.requestMatcher.matchMessage(input)) {
          // todo: test order of matching?
          underlying.observer.onNext(input)
          atLeastOneHandler = true
        }
      }

      if (!atLeastOneHandler && log.isTraceEnabled) {
        log.trace(s"Consumer #$consumerId. Skipped message from partiton#${next.partition}: $messageString")
      }
    }
    catch {
      case NonFatal(e) ⇒
        log.error(s"Consumer #$consumerId can't deserialize message from partiton#${next.partition}: $messageString", e)
    }
  }

  protected def consumeStream(stream: KafkaStream[Array[Byte], Array[Byte]]): Unit = {
    log.info(s"Starting consumer #$consumerId on topic ${route.kafkaTopic} -> ${underlyingSubscriptions.values.map(_.requestMatcher)}")
    try {
      val iterator = stream.iterator()
      while (iterator.hasNext()) {
        val next = iterator.next()
        consumeMessage(next)
      }
      log.info(s"Stopping consumer #$consumerId on topic ${route.kafkaTopic}")
    }
    catch {
      case NonFatal(t) ⇒
        log.error(s"Consumer #$consumerId failed", t)
    }
  }

  override def onEvent[REQ <: Request[Body]](matcher: RequestMatcher,
                       groupName: String,
                       inputDeserializer: RequestDeserializer[REQ],
                       subscriber: Observer[REQ]): Future[Subscription] = {

    routes.find(r ⇒ r.requestMatcher.matchRequestMatcher(matcher)) map { route ⇒
      val key = TopicSubscriptionKey(route.kafkaTopic, route.kafkaPartitionKeys, groupName)
      val underlyingSubscription = UnderlyingSubscription(matcher, inputDeserializer, subscriber)
      lock.synchronized {
        subscriptions.get(key) match {
          case Some(subscription: TopicSubscription[REQ] @unchecked) ⇒
            val nextId = subscription.addUnderlying(underlyingSubscription)
            Future.successful(KafkaTransportSubscription(key, nextId))

          case _ ⇒
            val subscription = new TopicSubscription[REQ](consumerProperties, encoding, 1, /*todo: per topic thread count*/
              route, groupName)
            subscriptions.put(key, subscription)
            val nextId = subscription.addUnderlying(underlyingSubscription)
            subscription.run()
            Future.successful(KafkaTransportSubscription(key, nextId))
        }
      }
    } getOrElse {
      Future.failed(new NoTransportRouteException(s"Kafka consumer (server). matcher: $matcher"))
    }
  }

  override def off(subscription: Subscription): Future[Unit] = {
    subscription match {
      case kafkaS: KafkaTransportSubscription ⇒
        val notFoundException = new IllegalArgumentException(s"Subscription not found: $subscription")
        lock.synchronized {
          subscriptions.get(kafkaS.key) match {
            case Some(s) ⇒
              if (s.removeUnderlying(kafkaS.underlyingId)) { // last subscription, terminate kafka subscription
                subscriptions.remove(kafkaS.key)
                Future {
                  s.stop(1.seconds)
                }
              }
              else {
                Future.successful{}
              }
            case None ⇒
              Future.failed(notFoundException)
          }
        }
      case _ ⇒
        Future.failed(new IllegalArgumentException(s"Expected KafkaTransportSubscription instead of: $subscription"))
    }
  }

  override def shutdown(duration: FiniteDuration): Future[Boolean] = {
    val futures = subscriptions.map { kv ⇒
      Future {
        kv._2.stop(duration)
      }
    }
    subscriptions.clear()
    Future.sequence(futures) map { _ ⇒
      true
    }
  }



class TopicSubscription(topicName: String,
                        consumerCfg: KafkaConsumerConfig,
                        scheduler: Scheduler,
                        lock: Object,
                        addCallback:(TopicSubscription) ⇒ Unit,
                        releaseCallback:(TopicSubscription) ⇒ Unit) {

  private var refCounter: Int = 0
  private var observableCancel: Option[Cancelable] = None
  private val subscriptions = new FuzzyIndex[EventSubjectSubscription]

  def createObservable(matcher: RequestMatcher, inputDeserializer: RequestDeserializer[RequestBase]): Observable[RequestBase] = {
    new EventSubjectSubscription(matcher,inputDeserializer)
      .observable
  }

  protected class EventSubjectSubscription(val requestMatcher: RequestMatcher,
                                           val inputDeserializer: RequestDeserializer[RequestBase])
    extends SubjectSubscription[RequestBase] {

    override protected val subject: Subject[eventType, eventType] = _
    // override protected val subject = ConcurrentSubject.publishToOne[CommandEvent[RequestBase]]

    override def remove(): Unit = {
      lock.synchronized {
        subscriptions.remove(this)
        refCounter -= 1
        if (refCounter <= 0) {
          observableCancel.foreach(_.cancel())
          releaseCallback(this)
        }
      }
    }

    override def add(): Unit = {
      lock.synchronized {
        subscriptions.add(this)
        refCounter += 1
        if (observableCancel.isEmpty) {
          observableCancel = Some {
            KafkaConsumerObservable[String,String](consumerCfg, List(topicName)).executeOn(scheduler).subscribe()
          }
          addCallback(this)
        }
      }
    }
  }
  */
