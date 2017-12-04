import java.io.Reader
import java.util.concurrent.atomic.AtomicInteger

import com.hypertino.binders.value.{Number, Obj, Text}
import com.hypertino.hyperbus.Hyperbus
import com.hypertino.hyperbus.model.annotations.{body, request}
import com.hypertino.hyperbus.model.{Body, DynamicBody, DynamicRequest, DynamicRequestObservableMeta, EmptyBody, HRL, Headers, MessagingContext, Method, Request}
import com.hypertino.hyperbus.serialization.RequestDeserializer
import com.hypertino.hyperbus.transport.KafkaPublishResult
import com.hypertino.hyperbus.transport.api.ServiceRegistrator
import com.hypertino.hyperbus.transport.api.matchers.RequestMatcher
import com.hypertino.hyperbus.transport.registrators.DummyRegistrator
import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.execution.Ack.Continue
import monix.execution.Scheduler
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}
import scaldi.Module

import scala.concurrent.Await
import scala.concurrent.duration._

class KafkaTransportTest extends FreeSpec with ScalaFutures with Matchers with BeforeAndAfter with Eventually {
  implicit val mcx = MessagingContext("123")
  implicit val scheduler = monix.execution.Scheduler.Implicits.global
  implicit val injector = new Module {
    bind [Scheduler] to scheduler
    bind [ServiceRegistrator] to DummyRegistrator
  }
  implicit val defaultPatience = PatienceConfig(timeout =  Span(5, Seconds), interval = Span(50, Millis))

  var hyperbus: Hyperbus = null
  before {
    hyperbus = new Hyperbus(ConfigFactory.load())(injector)
  }

  after {
    if (hyperbus != null) {
      Await.result(hyperbus.shutdown(10.seconds).runAsync, 10.seconds)
    }
  }

  "KafkaTransport " - {
    "Publish and then Subscribe" in {

      val subscriber1 = (request: DynamicRequest) ⇒ {
        Continue
      }

      // read previous messages if any
      val cancelable1 = hyperbus.events[DynamicRequest](Some("sub1"))(
        DynamicRequest.requestMeta,
        DynamicRequestObservableMeta(RequestMatcher("hb://test", Method.PUT, None))
      ).subscribe(subscriber1)
      Thread.sleep(3000)
      cancelable1.cancel()

      Thread.sleep(1000)

      val b1 = DynamicBody(Obj.from("test" → "12345"))
      val r1 = hyperbus.publish(DynamicRequest(HRL("hb://test", Obj.from("partition_id" → 1)), Method.PUT, b1)).runAsync.futureValue
      r1.size shouldBe 1
      r1.head shouldBe a[KafkaPublishResult]
      r1.head.offset should not be empty
      println(r1)
      //r1.head.asInstanceOf[KafkaPublishResult].committed.get should be > 0

      val b2 = DynamicBody(Obj.from("test" → "54321"))
      val r2 = hyperbus.publish(DynamicRequest(HRL("hb://test", Obj.from("partition_id" → 1)), Method.PUT, b2)).runAsync.futureValue
      r2.size shouldBe 1
      r2.head shouldBe a[KafkaPublishResult]
      r2.head.offset should not be empty
      println(r2)

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

      Thread.sleep(1000) // we need to wait until subscriptions will go acros the

      eventually {
        cnt.get should equal(4)
      }
      Thread.sleep(1000) // give chance to increment to another service (in case of wrong implementation)
      cnt.get should equal(4)

      cancelables.foreach(_.cancel())
    }
  }

  /*
  "Publish and then Subscribe with Same Group/Topic but different URI" in {
    import ExecutionContext.Implicits.global
    Thread.sleep(1000)

    val cnt = new AtomicInteger(0)

    val fsubs = mutable.MutableList[Future[Subscription]]()
    val subscriber1 = new Subscriber[MockRequest1]() {
      override def onNext(value: MockRequest1): Unit = {
        // TODO: fix this dirty hack. Message of type MockRequest1 should not be passed here as an instance of MockRequest2
        if (value.body.test == "12345")
          cnt.incrementAndGet()
      }
    }
    val subscriber2 = new Subscriber[MockRequest2]() {
      override def onNext(value: MockRequest2): Unit = {
        // TODO: fix this dirty hack. Message of type MockRequest1 should not be passed here as an instance of MockRequest2
        if (value.body.test == "54321")
          cnt.incrementAndGet()
      }
    }

    fsubs += transportManager.onEvent[MockRequest1](RequestMatcher(Some(Uri("/mock1/{partitionId}"))), "sub1", MockRequest1.apply, subscriber1)
    fsubs += transportManager.onEvent[MockRequest2](RequestMatcher(Some(Uri("/mock2/{partitionId}"))), "sub1", MockRequest2.apply, subscriber2)
    val subs = fsubs.map(_.futureValue)

    Thread.sleep(1000) // we need to wait until subscriptions will go acros the

    val f = Future.sequence(List(
      transportManager.publish(MockRequest1("1", MockBody("12345"))),
      transportManager.publish(MockRequest2("2", MockBody("54321"))))
    )

    val publishResults = f.futureValue
    publishResults.foreach { publishResult ⇒
      publishResult.sent should equal(Some(true))
      publishResult.offset shouldNot equal(None)
    }

    eventually {
      cnt.get should equal(2)
    }
    Thread.sleep(1000) // give chance to increment to another service (in case of wrong implementation)
    cnt.get should equal(2)

    subs.foreach(transportManager.off)
  }
  */
}

