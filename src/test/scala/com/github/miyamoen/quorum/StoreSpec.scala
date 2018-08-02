package com.github.miyamoen.quorum

import akka.actor.ActorSystem
import akka.testkit.{EventFilter, TestEvent, TestProbe}
import org.joda.time.DateTime
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.{BeforeAndAfterAll, Inspectors, Matchers, WordSpec}
import org.scalatest.Assertions._
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class StoreSpec extends BaseSpec {

  "Reading Store" should {
    "get an message" in {
      val someMessage = "some message"
      val store = system.actorOf(Store.props(Data.create(someMessage)))
      store ! Store.Read
      expectMsgPF() {
        case Data(message, _) =>
          assert(message == someMessage)
      }
    }
  }

}
