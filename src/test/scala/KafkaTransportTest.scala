import java.io.Reader
import java.util.concurrent.atomic.AtomicInteger

import com.hypertino.binders.value.Obj
import com.typesafe.config.ConfigFactory
import com.hypertino.hyperbus.model.annotations.{body, request}
import com.hypertino.hyperbus.model.{Body, DynamicRequest, MessagingContext, Method, Request, ResponseHeaders}
import com.hypertino.hyperbus.serialization.{RequestDeserializer, ResponseBaseDeserializer}
import com.hypertino.hyperbus.transport.api._
import com.hypertino.hyperbus.transport.api.matchers.RequestMatcher
import monix.eval.Task
import monix.execution.Ack.Continue
import monix.execution.Scheduler
import monix.reactive.observers.Subscriber
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}
import scaldi.Module

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

@body("mock")
case class MockBody(test: String) extends Body

@request(Method.POST, "hb://mock")
case class MockRequest(partitionId: String, body: MockBody) extends Request[MockBody]

@request(Method.POST, "hb://mock1")
case class MockRequest1(partitionId: String, body: MockBody) extends Request[MockBody]

@request(Method.POST, "hb://mock2")
case class MockRequest2(partitionId: String, body: MockBody) extends Request[MockBody]

class KafkaTransportTest extends FreeSpec with ScalaFutures with Matchers with BeforeAndAfter with Eventually {
  implicit val mcx = MessagingContext("123")
  val requestDeserializer: RequestDeserializer[MockRequest] = MockRequest.apply(_: Reader, _: Obj)
  implicit val scheduler = monix.execution.Scheduler.Implicits.global
  implicit val injector = new Module {
    bind [Scheduler] to scheduler
  }
  implicit val defaultPatience = PatienceConfig(timeout =  Span(5, Seconds), interval = Span(50, Millis))

  var transportManager: TransportManager = null
  before {
    val transportConfiguration = TransportConfigurationLoader.fromConfig(ConfigFactory.load(), injector)
    transportManager = new TransportManager(transportConfiguration)
  }

  after {
    if (transportManager != null) {
      Await.result(transportManager.shutdown(10.seconds).runAsync, 10.seconds)
    }
  }

  "KafkaTransport " - {
    "Publish and then Subscribe" in {

      import ExecutionContext.Implicits.global

      val subscriber1 = (request: MockRequest) ⇒ {
        Continue
      }

      // read previous messages if any
      val cancelable1 = transportManager.events[MockRequest](RequestMatcher("hb://mock", Method.POST), "sub1", MockRequest.apply).subscribe(subscriber1)
      Thread.sleep(3000)
      cancelable1.cancel()

      Thread.sleep(1000)

      Task.gatherUnordered(List(
        transportManager.publish(MockRequest("1", MockBody("12345"))),
        transportManager.publish(MockRequest("2", MockBody("54321"))))
      ).runOnComplete { r ⇒
        r.get.foreach { publishResult ⇒
          publishResult.sent should equal(Some(true))
          publishResult.offset shouldNot equal(None)
        }
      }

      val cnt = new AtomicInteger(0)

      val subscriber2 = (request: MockRequest) ⇒ {
        request.body.test should (equal("12345") or equal("54321"))
        cnt.incrementAndGet()
        Continue
      }

      val cancelables = List(
        transportManager.events[MockRequest](RequestMatcher("hb://mock", Method.POST), "sub1", MockRequest.apply).subscribe(subscriber2),
        transportManager.events[MockRequest](RequestMatcher("hb://mock", Method.POST), "sub1", MockRequest.apply).subscribe(subscriber2),
        transportManager.events[MockRequest](RequestMatcher("hb://mock", Method.POST), "sub2", MockRequest.apply).subscribe(subscriber2)
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

