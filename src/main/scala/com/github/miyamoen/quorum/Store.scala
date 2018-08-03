package com.github.miyamoen.quorum

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

object Store {
  def props(message: Message) = Props(new Store(message))

  def createStores(n: Int, message: Message): List[Props] =
    (1 to n).map(_ => props(message)).toList

  sealed trait Op
  case class Write(message: Message) extends Op
  case object Read extends Op
  case object Lock extends Op
  case object Release extends Op

  sealed trait Status
  case object Succeeded extends Status
  case object Failed extends Status
}

class Store(var message: Message) extends Actor with ActorLogging {

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
    case Write(message) if owner.contains(sender()) =>
      this.message = message
      context.become(open)
      sender() ! Succeeded

    case Read if owner.contains(sender()) =>
      sender() ! message
      context.become(open)

    case Release if owner.contains(sender()) =>
      owner = None
      context.become(open)
      sender() ! Succeeded

    case _ =>
      sender() ! Failed

  }
}
