package com.github.miyamoen.quorum

import akka.actor.{ActorRef, LoggingFSM, Props}
import com.github.nscala_time.time.Imports._

object Quorum {
  def props(stores: List[ActorRef]) = Props(new Quorum(stores))

  sealed trait Operation

  case object Read extends Operation

  case class Write(message: String) extends Operation

  sealed trait Result

  case object Succeeded extends Result

  case object Failed extends Result

  sealed trait State

  case object Open extends State

  case object Locking extends State

  case object Writing extends State

  case object Reading extends State

  case object Releasing extends State


  sealed trait Data

  case object Empty extends Data

  case class LockCount(writeMessage: Option[String], rest: List[ActorRef], locked: List[ActorRef], replyTo: ActorRef) extends Data

  case class ReleaseCount(count: Int, writeMessage: Option[String], replyTo: ActorRef) extends Data

  case class Messages(messages: List[Message], replyTo: ActorRef) extends Data

  case class WriteCount(count: Int, replyTo: ActorRef) extends Data

}

class Quorum(stores: List[ActorRef])
  extends LoggingFSM[Quorum.State, Quorum.Data] {

  import Quorum._

  startWith(Open, Empty)

  when(Open) {
    case Event(Read, _) =>
      goto(Locking) using LockCount(writeMessage = None, rest = stores.tail, locked = Nil, sender())

    case Event(Write(message), _) =>
      goto(Locking) using LockCount(writeMessage = Some(message), rest = stores.tail, locked = Nil, sender())
  }
  when(Locking) {
    case Event(Store.Succeeded(_), LockCount(None, rest, locked, replyTo))
      if rest.isEmpty && locked.size + 1 == stores.size =>
      log.debug("Quorum finish lock to Read")
      goto(Reading) using Messages(Nil, replyTo)

    case Event(Store.Succeeded(_), LockCount(Some(_), rest, locked, replyTo))
      if rest.isEmpty && locked.size + 1 == stores.size =>
      log.debug("Quorum finish lock to Write")
      goto(Writing) using WriteCount(0, replyTo)

    case Event(Store.Succeeded(_), LockCount(writeMessage, rest, locked, replyTo)) =>
      rest.head ! Store.Lock
      stay() using LockCount(writeMessage, rest = rest.tail, locked = sender() :: locked, replyTo)

    case Event(Store.Failed(_), LockCount(writeMessage, _, locked, replyTo)) if locked.nonEmpty =>
      log.debug("Quorum locked store count: {}", locked.size)
      goto(Releasing) using ReleaseCount(locked.size, writeMessage, replyTo)

    case Event(Store.Failed(_), LockCount(writeMessage, _, locked, replyTo)) if locked.isEmpty =>
      goto(Locking) using LockCount(writeMessage, rest = stores.tail, locked = Nil, replyTo)
  }

  when(Releasing) {
    case Event(_: Store.Result, ReleaseCount(count, writeMessage, replyTo)) if count - 1 == 0 =>
      log.debug("Quorum finish release")
      goto(Locking) using LockCount(writeMessage, rest = stores.tail, locked = Nil, replyTo)

    case Event(_: Store.Result, ReleaseCount(count, writeMessage, replyTo)) =>
      stay() using ReleaseCount(count - 1, writeMessage, replyTo)
  }

  when(Reading) {
    case Event(message: Message, Messages(messages, replyTo)) if messages.size + 1 >= stores.size =>
      replyTo ! (message :: messages).maxBy(msg => msg.timestamp)
      goto(Open) using Empty

    case Event(message: Message, Messages(messages, replyTo)) =>
      stay() using Messages(message :: messages, replyTo)
  }

  when(Writing) {
    case Event(Store.Succeeded(_), WriteCount(count, replyTo)) if count + 1 == stores.size =>
      replyTo ! Succeeded
      goto(Open) using Empty

    case Event(Store.Succeeded(_), WriteCount(count, replyTo)) =>
      stay() using WriteCount(count + 1, replyTo)
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

    case Releasing -> Locking =>
      log.debug("Quorum Retry Locking")
      stores.head ! Store.Lock

    case Locking -> Locking =>
      log.debug("Quorum Immediately Retry Locking")
      stores.head ! Store.Lock

    case Locking -> Reading =>
      log.debug("Quorum Read")
      read()

    case Locking -> Writing =>
      log.debug("Quorum Write")
      stateData match {
        case LockCount(Some(message), _, _, _) =>
          write(Message(message))
      }
  }

  private def read(): Unit = {
    stores.foreach(store => store ! Store.Read)
  }

  private def write(message: Message): Unit = {
    stores.foreach(store => store ! Store.Write(message))
  }

}
