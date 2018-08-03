package com.github.miyamoen.quorum

import akka.actor.{ActorRef, LoggingFSM}

object Quorum {

  sealed trait Op
  case object Read extends Op
  case class Write(message: String) extends Op

  sealed trait Status
  case object Succeeded extends Status
  case object Failed extends Status

  sealed trait State
  case object Open extends State
  case class Locking(op: Op) extends State
  case object Writing extends State
  case object Reading extends State

  sealed trait Data
  case object Empty extends Data
  case class Count(count: Int, address: ActorRef) extends Data
  case class Messages(messages: List[Message], address: ActorRef) extends Data
  case class Envelope(message: Message, address: ActorRef) extends Data

}

class Quorum(stores: List[ActorRef])
    extends LoggingFSM[Quorum.State, Quorum.Data] {

  import Quorum._

  startWith(Open, Empty)

  when(Open) {
    case Event(Read, _) =>
      goto(Locking(Read)) using Count(0, sender())

    case Event(Write(message), _) =>
      goto(Locking(Write(message))) using Count(0, sender())
  }

  when(Locking(Read))(locking(Read))

  def locking(op: Op): StateFunction = {
    case Event(Store.Succeeded, Count(count, address))
        if count + 1 == stores.size =>
      op match {
        case Read =>
          goto(Reading) using Messages(List.empty, address)

        case Write(message) =>
          goto(Writing) using Envelope(Message.create(message), address)
      }

    case Event(Store.Succeeded, Count(count, address)) =>
      stay() using Count(count + 1, address)

    case Event(Store.Failed, Count(_, address)) =>
      address ! Failed
      goto(Open) using Empty
  }

  when(Reading) {
    case Event(message: Message, Messages(messages, address))
        if messages.size + 1 >= stores.size =>
      address ! (message :: messages).maxBy(msg => msg.timestamp)
      goto(Open) using Empty

    case Event(message: Message, Messages(messages, address)) =>
      stay() using Messages(message :: messages, address)
  }

  when(Writing) {
    case Event(Store.Succeeded, Envelope(_, address)) =>
      stay() using Count(0, address)

    case Event(Store.Succeeded, Count(count, address))
        if count + 1 == stores.size =>
      address ! Succeeded
      goto(Open) using Empty

    case Event(Store.Succeeded, Count(count, address)) =>
      stay() using Count(count + 1, address)
  }

  onTransition {
    case Open -> Locking(_) =>
      lock()

    case Locking(_) -> Open =>
      release()

    case Locking(_) -> Reading =>
      read()

    case Locking(_) -> Writing =>
      stateData match {
        case Envelope(message: Message, _) =>
          write(message)
      }
  }

  def release(): Unit = {
    stores.foreach(store => store ! Store.Release)
  }

  def lock(): Unit = {
    stores.foreach(store => store ! Store.Lock)
  }

  def read(): Unit = {
    stores.foreach(store => store ! Store.Read)
  }

  def write(message: Message): Unit = {
    stores.foreach(store => store ! Store.Write(message))
  }

}
