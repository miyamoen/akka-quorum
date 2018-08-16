package com.github.miyamoen.quorum

import akka.actor.{ActorRef, LoggingFSM, Props}
import com.github.nscala_time.time.Imports._

object Quorum {
  def props(stores: List[ActorRef]) = Props(new Quorum(stores))

  sealed trait Op

  case object Read extends Op

  case class Write(message: String) extends Op

  sealed trait Status

  case object Succeeded extends Status

  case object Failed extends Status

  sealed trait State

  case object Open extends State

  case object Locking extends State

  case object Writing extends State

  case object Reading extends State

  case object Releasing extends State


  sealed trait Data

  case object Empty extends Data


  case class LockCount(writeMessage: Option[String], rest: List[ActorRef], locked: List[ActorRef], address: ActorRef) extends Data

  case class ReleaseCount(count: Int) extends Data

  case class Messages(messages: List[Message], address: ActorRef) extends Data

  case class Envelope(message: Message, address: ActorRef) extends Data

  case class WriteCount(count: Int, address: ActorRef) extends Data

}

class Quorum(stores: List[ActorRef])
  extends LoggingFSM[Quorum.State, Quorum.Data] {

  import Quorum._

  startWith(Open, Empty)

  when(Open) {
    case Event(Read, _) =>
      goto(Locking) using LockCount(None, stores.tail, Nil, sender())

    case Event(Write(message), _) =>
      goto(Locking) using LockCount(Some(message), stores.tail, Nil, sender())
  }
  when(Locking) {
    case Event(Store.Succeeded(_), LockCount(None, rest, locked, address))
      if rest.isEmpty && locked.size + 1 == stores.size =>
      goto(Reading) using Messages(Nil, address)

    case Event(Store.Succeeded(_), LockCount(Some(_), rest, locked, address))
      if rest.isEmpty && locked.size + 1 == stores.size =>
      goto(Writing) using WriteCount(0, address)

    case Event(Store.Succeeded(_), LockCount(writeMessage, rest, locked, address)) =>
      rest.head ! Store.Lock
      stay() using LockCount(writeMessage, rest.tail, sender() :: locked, address)

    case Event(Store.Failed(_), LockCount(_, _, locked, address)) =>
      log.debug("write lock count: {}", locked.size)
      address ! Failed
      goto(Releasing) using ReleaseCount(locked.size)

  }

  when(Releasing) {
    case Event(_: Store.Status, ReleaseCount(count)) if count - 1 == 0 =>
      log.debug("finish release count: {}", count)
      goto(Open) using Empty

    case Event(_: Store.Status, ReleaseCount(count)) =>
      log.debug("release count: {}", count)
      stay() using ReleaseCount(count - 1)
  }

  when(Reading) {
    case Event(message: Message, Messages(messages, address)) if messages.size + 1 >= stores.size =>
      address ! (message :: messages).maxBy(msg => msg.timestamp)
      goto(Open) using Empty

    case Event(message: Message, Messages(messages, address)) =>
      stay() using Messages(message :: messages, address)
  }

  when(Writing) {
    case Event(Store.Succeeded(_), WriteCount(count, address)) if count + 1 == stores.size =>
      address ! Succeeded
      goto(Open) using Empty

    case Event(Store.Succeeded(_), WriteCount(count, address)) =>
      stay() using WriteCount(count + 1, address)
  }

  onTransition {
    case Open -> Locking =>
      log.debug("Quorum Lock")
      stores.head ! Store.Lock


    case Locking -> Releasing =>
      stateData match {
        case LockCount(_, _, locked, _) =>
          log.debug("Quorum Lock Release")
          locked.foreach(store => store ! Store.Release)
      }

    case Locking -> Reading =>
      log.debug("Quorum Read")
      read()

    case Locking -> Writing =>
      log.debug("Quorum Write")
      stateData match {
        case LockCount(Some(message), _, _, _) =>
          write(Message.create(message))
      }
  }

  private def read(): Unit = {
    stores.foreach(store => store ! Store.Read)
  }

  private def write(message: Message): Unit = {
    stores.foreach(store => store ! Store.Write(message))
  }

}
