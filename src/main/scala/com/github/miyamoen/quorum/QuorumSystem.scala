package com.github.miyamoen.quorum

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

import scala.util.Random

object QuorumSystem {
  def props() = Props(new QuorumSystem)

  val size = 10

}

class QuorumSystem extends Actor with ActorLogging {

  import QuorumSystem._

  val stores: List[ActorRef] =
    Store.createStoreProps(size, Message("initial message"))
      .map(store => context.actorOf(store))

  val quorums: List[ActorRef] = (1 to 200).map(_ => context.actorOf(Quorum.props(Random.shuffle(stores).take(size / 2 + 1)))).toList

  override def receive: Receive = {
    case op: Quorum.Operation =>
      val index = Random.nextInt(quorums.length)
      quorums(index) forward op
  }
}
