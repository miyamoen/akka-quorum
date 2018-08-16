package com.github.miyamoen.quorum

import akka.actor.ActorRef

class QuorumSpec extends BaseSpec {
  "Quorum" should {
    "be read" in {
      val msg = Message("initial")
      val stores: List[ActorRef] = Store
        .createStores(10, msg)
        .map(store => system.actorOf(store))
      val quorum = system.actorOf(Quorum.props(stores))

      quorum ! Quorum.Read
      expectMsg(msg)
    }

    "be written" in {
      val msg = Message("initial")
      val stores: List[ActorRef] = Store
        .createStores(10, msg)
        .map(store => system.actorOf(store))
      val quorum = system.actorOf(Quorum.props(stores))

      quorum ! Quorum.Write("update")
      expectMsg(Quorum.Succeeded)

      quorum ! Quorum.Read
      expectMsgPF() {
        case Message(message, _) =>
          assert(message == "update")
      }
    }

    "be written and read" in {
      val msg = Message("initial")
      val shared: ActorRef = system.actorOf(Store.props(msg))
      val quorum0 = system.actorOf(
        Quorum.props(
          shared :: Store
            .createStores(10, msg)
            .map(store => system.actorOf(store))))
      val quorum1 = system.actorOf(
        Quorum.props(
          shared :: Store
            .createStores(10, msg)
            .map(store => system.actorOf(store))))

      quorum0 ! Quorum.Read
      quorum1 ! Quorum.Read

      expectMsg(msg)
      expectMsg(msg)
    }
  }

}
