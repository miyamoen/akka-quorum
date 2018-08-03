package com.github.miyamoen.quorum

import akka.actor.ActorRef
import akka.testkit.TestProbe

class StoreSpec extends BaseSpec {
  val data = Data.create("some message")
  val initialData = Data.create("initial message")

  "Opened store" should {
    "be locked" in {
      val store = system.actorOf(Store.props(initialData))
      store ! Store.Lock
      expectMsg(Store.Succeeded)
    }

    "not be operated" in {
      val store = system.actorOf(Store.props(initialData))
      store ! Store.Read
      expectMsg(Store.Failed)

      store ! Store.Write(data)
      expectMsg(Store.Failed)

      store ! Store.Release
      expectMsg(Store.Failed)

      store ! "hogehoge"
      expectMsg(Store.Failed)
    }
  }

  def createLockedStore(): ActorRef = {
    val store = system.actorOf(Store.props(initialData))
    store ! Store.Lock
    expectMsg(Store.Succeeded)
    store
  }

  "Locked store" should {
    "be released" in {
      val store = createLockedStore
      store ! Store.Release
      expectMsg(Store.Succeeded)
    }

    "be read" in {
      val store = createLockedStore
      store ! Store.Read
      expectMsg(initialData)
    }
    "be written" in {
      val store = createLockedStore
      store ! Store.Write(data)
      expectMsg(Store.Succeeded)

      store ! Store.Lock
      expectMsg(Store.Succeeded)

      store ! Store.Read
      expectMsg(data)
    }

    "not be operated" in {
      val store = createLockedStore
      store ! Store.Lock
      expectMsg(Store.Failed)

      store ! "hogehoge"
      expectMsg(Store.Failed)

    }
  }

  "Other actor" should {
    "not lock locked store" in {
      val store = createLockedStore
      val other = TestProbe()

      store.tell(Store.Lock, other.ref)
      other.expectMsg(Store.Failed)
    }

    "not operate localed store" in {
      val store = system.actorOf(Store.props(initialData))
      val other = TestProbe()

      store.tell( Store.Read,other.ref)
      other.expectMsg(Store.Failed)

      store.tell( Store.Write(data),other.ref)
      other.expectMsg(Store.Failed)

      store.tell( Store.Release,other.ref)
      other.expectMsg(Store.Failed)

      store.tell( "hogehoge",other.ref)
      other.expectMsg(Store.Failed)
    }
  }

}
