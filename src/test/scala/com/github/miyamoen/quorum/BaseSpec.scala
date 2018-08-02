package com.github.miyamoen.quorum

import akka.actor.ActorSystem
import akka.testkit.{EventFilter, ImplicitSender, TestEvent, TestKit}
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

abstract class BaseSpec extends TestKit(ActorSystem("MySpec"))
  with ImplicitSender with WordSpecLike with Matchers
  with TypeCheckedTripleEquals with Inspectors with BeforeAndAfterAll {

  system.eventStream.publish(TestEvent.Mute(EventFilter.debug()))
  system.eventStream.publish(TestEvent.Mute(EventFilter.info()))
  system.eventStream.publish(TestEvent.Mute(EventFilter.warning()))
  system.eventStream.publish(TestEvent.Mute(EventFilter.error()))

  override protected def afterAll(): Unit = {
    Await.ready(system.terminate(), 20 seconds)
  }
}
