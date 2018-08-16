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
  case class Succeeded(message: String) extends Status
  case class Failed(message: String) extends Status
}

class Store(var message: Message) extends Actor with ActorLogging {

  import Store._

  var owner: Option[ActorRef] = None

  override def receive: Receive = open

  def open: Receive = {
    case Lock =>
      log.debug("Locked by {}", sender())
      owner = Some(sender())
      context.become(lock)
      sender() ! Succeeded("Lock")

    case op =>
      sender() ! Failed(op.toString)
  }

  def lock: Receive = {
    case Write(message) if owner.contains(sender()) =>
      log.debug("Written by {}", sender())
      this.message = message
      context.become(open)
      sender() ! Succeeded("Write")

    case Read if owner.contains(sender()) =>
      log.debug("Read by {}", sender())
      sender() ! message
      context.become(open)

    case Release if owner.contains(sender()) =>
      log.debug("Released by {}", sender())
      owner = None
      context.become(open)
      sender() ! Succeeded("Release")

    case op =>
      sender() ! Failed(op.toString)

  }
}
