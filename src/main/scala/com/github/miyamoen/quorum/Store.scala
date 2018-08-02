package com.github.miyamoen.quorum

import akka.actor.{Actor, ActorLogging, Props}


object Store {
  def props(data: Data) = Props(new Store(data))

  case class Write(data: Data)

  case object Read


}

class Store(var data: Data) extends Actor with ActorLogging {

  import Store._

  override def receive: Receive = {
    case Write(data) =>
      this.data = data
    case Read =>
      sender() ! data
  }
}
