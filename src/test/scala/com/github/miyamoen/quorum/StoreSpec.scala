package com.github.miyamoen.quorum

import akka.actor.ActorRef
import akka.testkit.TestProbe

class StoreSpec extends BaseSpec {
  val message: Message = Message.create("some message")
  val initialMessage: Message = Message.create("initial message")

  "Opened store" should {
    "be locked" in {
      val store = system.actorOf(Store.props(initialMessage))

      store ! Store.Lock
      expectMsg(Store.Succeeded("Lock"))
    }

    "not be operated" in {
      val store = system.actorOf(Store.props(initialMessage))
      store ! Store.Read
      expectMsg(Store.Failed("Read"))

      store ! Store.Write(message)
      expectMsg(Store.Failed(Store.Write(message).toString))

      store ! Store.Release
      expectMsg(Store.Failed("Release"))

      store ! "hogehoge"
      expectMsg(Store.Failed("hogehoge"))
    }
  }

  def createLockedStore(): ActorRef = {
    val store = system.actorOf(Store.props(initialMessage))
    store ! Store.Lock
    expectMsg(Store.Succeeded("Lock"))
    store
  }

  "Locked store" should {
    "be released" in {
      val store = createLockedStore()
      store ! Store.Release
      expectMsg(Store.Succeeded("Release"))
    }

    "be read" in {
      val store = createLockedStore()
      store ! Store.Read
      expectMsg(initialMessage)
    }
    "be written" in {
      val store = createLockedStore()
      store ! Store.Write(message)
      expectMsg(Store.Succeeded("Write"))

      store ! Store.Lock
      expectMsg(Store.Succeeded("Lock"))

      store ! Store.Read
      expectMsg(message)
    }

    "not be operated" in {
      val store = createLockedStore()
      store ! Store.Lock
      expectMsg(Store.Failed("Lock"))

      store ! "hogehoge"
      expectMsg(Store.Failed("hogehoge"))

    }
  }

  "Other actor" should {
    "not lock locked store" in {
      val store = createLockedStore()
      val other = TestProbe()

      store.tell(Store.Lock, other.ref)
      other.expectMsg(Store.Failed("Lock"))
    }

    "not operate localed store" in {
      val store = system.actorOf(Store.props(initialMessage))
      val other = TestProbe()

      store.tell(Store.Read, other.ref)
      other.expectMsg(Store.Failed("Read"))

      store.tell(Store.Write(message), other.ref)
      other.expectMsg(Store.Failed(Store.Write(message).toString))

      store.tell(Store.Release, other.ref)
      other.expectMsg(Store.Failed("Release"))

      store.tell("hogehoge", other.ref)
      other.expectMsg(Store.Failed("hogehoge"))
    }
  }

}
