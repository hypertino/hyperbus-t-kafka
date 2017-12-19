import java.util.concurrent.atomic.AtomicInteger

import com.hypertino.binders.value.{Number, Obj, Text}
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.{DynamicBody, DynamicRequest, DynamicRequestObservableMeta, HRL, MessagingContext, Method}
import com.hypertino.hyperbus.transport.KafkaPublishResult
import com.hypertino.hyperbus.transport.api.ServiceRegistrator
import com.hypertino.hyperbus.transport.api.matchers.RequestMatcher
import com.hypertino.hyperbus.transport.kafkatransport.ConfigLoader
import com.hypertino.hyperbus.transport.registrators.DummyRegistrator
import com.typesafe.config.ConfigFactory
import monix.execution.Ack.Continue
import monix.execution.Scheduler
import monix.kafka.KafkaConsumerObservable
import monix.reactive.Observer
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterEach, FreeSpec, Matchers}
import scaldi.Module

import scala.concurrent.Await
import scala.concurrent.duration._

class KafkaTransportTest extends FreeSpec with ScalaFutures with Matchers with BeforeAndAfterEach with Eventually {
  implicit val mcx = MessagingContext("123")
  implicit val scheduler = monix.execution.Scheduler.Implicits.global
  implicit val injector = new Module {
    bind [Scheduler] to scheduler
    bind [ServiceRegistrator] to DummyRegistrator
  }
  implicit val defaultPatience = PatienceConfig(timeout =  Span(60, Seconds), interval = Span(200, Millis))

  val config = ConfigFactory.load()
  var hyperbus: Hyperbus = null
  override def beforeEach {
    hyperbus = new Hyperbus(config)(injector)
  }

  override def afterEach {
    if (hyperbus != null) {
      Await.result(hyperbus.shutdown(10.seconds).runAsync, 10.seconds)
    }
    Thread.sleep(10000)
  }

  def consumeAll(group: String, topic: String, wait: Int=3000) = {
    val kafkaConsumer = KafkaConsumerObservable[String,String](ConfigLoader.loadConsumerConfig(config).copy(groupId=group), List(topic))
    val cancelable = kafkaConsumer
      .subscribe(Observer.dump(s"consume-$group"))
    Thread.sleep(2*wait/3)
    cancelable.cancel()
    Thread.sleep(wait/3)
  }

  "KafkaTransport " - {
    "Publish and then Subscribe" in {
      consumeAll("sub1", "hyperbus-test")
      consumeAll("sub2", "hyperbus-test")
      Thread.sleep(10000)

      val b1 = DynamicBody(Obj.from("test" → "12345"))
      val r1 = hyperbus.publish(DynamicRequest(HRL("hb://test", Obj.from("partition_id" → 1)), Method.PUT, b1)).runAsync.futureValue
      r1.size shouldBe 1
      r1.head shouldBe a[KafkaPublishResult]
      r1.head.offset should not be empty

      val b2 = DynamicBody(Obj.from("test" → "54321"))
      val r2 = hyperbus.publish(DynamicRequest(HRL("hb://test", Obj.from("partition_id" → 1)), Method.PUT, b2)).runAsync.futureValue
      r2.size shouldBe 1
      r2.head shouldBe a[KafkaPublishResult]
      r2.head.offset should not be empty

      val cnt = new AtomicInteger(0)

      val subscriber2 = (request: DynamicRequest) ⇒ {
        request.body.content.dynamic.test should (equal(Text("12345")) or equal(Text("54321")))
        cnt.incrementAndGet()
        Continue
      }

      val om = DynamicRequestObservableMeta(RequestMatcher("hb://test", Method.PUT, None))

      val cancelables = List(
        hyperbus.events[DynamicRequest](Some("sub1"))(DynamicRequest.requestMeta, om).subscribe(subscriber2),
        hyperbus.events[DynamicRequest](Some("sub1"))(DynamicRequest.requestMeta, om).subscribe(subscriber2),
        hyperbus.events[DynamicRequest](Some("sub2"))(DynamicRequest.requestMeta, om).subscribe(subscriber2)
      )

      hyperbus.startServices()

      eventually {
        cnt.get should equal(4)
      }
      Thread.sleep(1000) // give chance to increment to another service (in case of wrong implementation)
      cnt.get should equal(4)

      cancelables.foreach(_.cancel())
    }
  }

  "Publish and then Subscribe with Same Group/Topic but different URI" in {
    consumeAll("sub1", "hyperbus-testa")
    consumeAll("sub2", "hyperbus-testa")
    Thread.sleep(10000)

    val b1 = DynamicBody(Obj.from("test" → "12345"))
    val r1 = hyperbus.publish(DynamicRequest(HRL("hb://testa1", Obj.from("partition_id" → 1)), Method.PUT, b1)).runAsync.futureValue
    r1.size shouldBe 1
    r1.head shouldBe a[KafkaPublishResult]
    r1.head.offset should not be empty

    val b2 = DynamicBody(Obj.from("test" → "54321"))
    val r2 = hyperbus.publish(DynamicRequest(HRL("hb://testa2", Obj.from("partition_id" → 1)), Method.PUT, b2)).runAsync.futureValue
    r2.size shouldBe 1
    r2.head shouldBe a[KafkaPublishResult]
    r2.head.offset should not be empty

    val cnt = new AtomicInteger(0)

    val subscriber2 = (request: DynamicRequest) ⇒ {
      request.body.content.dynamic.test should (equal(Text("12345")) or equal(Text("54321")))
      cnt.incrementAndGet()
      Continue
    }

    val om1 = DynamicRequestObservableMeta(RequestMatcher("hb://testa1", Method.PUT, None))
    val om2 = DynamicRequestObservableMeta(RequestMatcher("hb://testa2", Method.PUT, None))

    val cancelables = List(
      hyperbus.events[DynamicRequest](Some("sub1"))(DynamicRequest.requestMeta, om1).subscribe(subscriber2),
      hyperbus.events[DynamicRequest](Some("sub1"))(DynamicRequest.requestMeta, om2).subscribe(subscriber2),
      hyperbus.events[DynamicRequest](Some("sub2"))(DynamicRequest.requestMeta, om1).subscribe(subscriber2)
    )

    hyperbus.startServices()

    eventually {
      cnt.get should equal(3)
    }
    Thread.sleep(1000) // give chance to increment to another service (in case of wrong implementation)
    cnt.get should equal(3)

    cancelables.foreach(_.cancel())
  }
}

