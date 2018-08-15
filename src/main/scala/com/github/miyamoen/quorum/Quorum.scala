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

  case object ReadingLocking extends State

  case object WritingLocking extends State

  case object Writing extends State

  case object Reading extends State

  case object Releasing extends State


  sealed trait Data

  case object Empty extends Data

  case class ReadingLockCount(rest: List[ActorRef], locked: List[ActorRef], address: ActorRef) extends Data

  case class WritingLockCount(message: String, rest: List[ActorRef], locked: List[ActorRef], address: ActorRef) extends Data

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
      goto(ReadingLocking) using ReadingLockCount(stores.tail, Nil, sender())

    case Event(Write(message), _) =>
      goto(WritingLocking) using WritingLockCount(message, stores.tail, Nil, sender())
  }

  when(ReadingLocking) {
    case Event(Store.Succeeded, ReadingLockCount(rest, locked, address))
      if rest.isEmpty && locked.size + 1 == stores.size =>
      goto(Reading) using Messages(Nil, address)

    case Event(Store.Succeeded, ReadingLockCount(rest, locked, address)) =>
      rest.head ! Store.Lock
      stay() using ReadingLockCount(rest.tail, sender() :: locked, address)

    case Event(Store.Failed, ReadingLockCount(_, locked, _)) =>
      goto(Releasing) using ReleaseCount(locked.size)
  }

  when(WritingLocking) {
    case Event(Store.Succeeded, WritingLockCount(_, rest, locked, address))
      if rest.isEmpty && locked.size + 1 == stores.size =>
      goto(Writing) using WriteCount(0, address)

    case Event(Store.Succeeded, WritingLockCount(message, rest, locked, address)) =>
      rest.head ! Store.Lock
      stay() using WritingLockCount(message, rest.tail, sender() :: locked, address)

    case Event(Store.Failed, WritingLockCount(_, _, locked, _)) =>
      goto(Releasing) using ReleaseCount(locked.size)

  }

  when(Releasing) {
    case Event(_: Store.Status, ReleaseCount(count)) if count - 1 == 0 =>
      goto(Open) using Empty

    case Event(_: Store.Status, ReleaseCount(count)) =>
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
    case Event(Store.Succeeded, WriteCount(count, address)) if count + 1 == stores.size =>
      address ! Succeeded
      goto(Open) using Empty

    case Event(Store.Succeeded, WriteCount(count, address)) =>
      stay() using WriteCount(count + 1, address)
  }

  onTransition {
    case Open -> ReadingLocking =>
      log.debug("Quorum Locking")
      stores.head ! Store.Lock

    case Open -> WritingLocking =>
      stores.head ! Store.Lock

    case ReadingLocking -> Releasing =>
      log.debug("Quorum Read Lock Release")
      stateData match {
        case ReadingLockCount(_, locked, _) =>
          locked.foreach(store => store ! Store.Release)
      }
    case WritingLocking -> Releasing =>
      log.debug("Quorum Write Lock Release")
      stateData match {
        case WritingLockCount(_, successes, _, _) =>
          successes.foreach(store => store ! Store.Release)
      }

    case ReadingLocking -> Reading =>
      log.debug("Quorum Read")
      read()

    case WritingLocking -> Writing =>
      log.debug("Quorum Write")
      stateData match {
        case WritingLockCount(message, _, _, _) =>
          write(Message.create(message))
      }
  }

  private def lock(): Unit = {
    stores.foreach(store => store ! Store.Lock)
  }

  private def read(): Unit = {
    stores.foreach(store => store ! Store.Read)
  }

  private def write(message: Message): Unit = {
    stores.foreach(store => store ! Store.Write(message))
  }

}
