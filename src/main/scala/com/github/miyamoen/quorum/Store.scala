package com.github.miyamoen.quorum

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

object Store {
  def props(data: Data) = Props(new Store(data))

  case class Write(data: Data)

  case object Read

  case object Lock

  case object Release

  case object Succeeded

  case object Failed

}

class Store(var data: Data) extends Actor with ActorLogging {

  import Store._

  var owner: Option[ActorRef] = None

  override def receive: Receive = open

  def open: Receive = {
    case Lock =>
      owner = Some(sender())
      context.become(lock)
      sender() ! Succeeded

    case _ =>
      sender() ! Failed
  }

  def lock: Receive = {
    case Write(data) if owner.contains(sender())=>
      this.data = data
      context.become(open)
      sender() ! Succeeded

    case Read if owner.contains(sender())=>
      sender() ! data
      context.become(open)

    case Release if owner.contains(sender())=>
      owner = None
      context.become(open)
      sender() ! Succeeded

    case _ =>
      sender() ! Failed

  }
}
