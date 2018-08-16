package com.github.miyamoen.quorum

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

object Store {
  def props(message: Message) = Props(new Store(message))

  def createStores(n: Int, message: Message): List[Props] =
    (1 to n).map(_ => props(message)).toList

  sealed trait Operation

  case class Write(message: Message) extends Operation

  case object Read extends Operation

  case object Lock extends Operation

  case object Release extends Operation

  sealed trait Result

  case class Succeeded(message: String) extends Result

  case class Failed(message: String) extends Result

}

class Store(initialMessage: Message) extends Actor with ActorLogging {
  var message: Message = initialMessage

  import Store._

  override def receive: Receive = open

  def open: Receive = {
    case Lock =>
      log.debug("Locked by {}", sender())
      context.become(lock(sender()))
      sender() ! Succeeded("Lock")

    case op =>
      sender() ! Failed(op.toString)
  }

  def lock(owner: ActorRef): Receive = {
    case Write(updateMessage) if owner == sender() =>
      log.debug("Written by {}", sender())
      this.message = updateMessage
      context.become(open)
      sender() ! Succeeded("Write")

    case Read if owner == sender() =>
      log.debug("Read by {}", sender())
      sender() ! message
      context.become(open)

    case Release if owner == sender() =>
      log.debug("Released by {}", sender())
      context.become(open)
      sender() ! Succeeded("Release")

    case op =>
      sender() ! Failed(op.toString)

  }
}
