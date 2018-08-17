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

  case class LockCount(writeMessage: Option[String], rest: List[ActorRef], locked: List[ActorRef], replyTo: ActorRef) extends Data {
    def isComplete(stores: List[ActorRef]): Boolean = rest.isEmpty && locked.size + 1 == stores.size

    def hasLockedStores: Boolean = locked.nonEmpty
  }

  case class ReleaseCount(count: Int, writeMessage: Option[String], replyTo: ActorRef) extends Data {
    def isComplete: Boolean = count - 1 == 0
  }

  case class MessageCount(messages: List[Message], replyTo: ActorRef) extends Data {
    def isComplete(stores: List[ActorRef]): Boolean = messages.size + 1 >= stores.size
  }

  case class WriteCount(count: Int, replyTo: ActorRef) extends Data {
    def isComplete(stores: List[ActorRef]): Boolean = count + 1 == stores.size
  }

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
    case Event(Store.Succeeded(_), lockCount@LockCount(None, _, _, replyTo)) if lockCount.isComplete(stores) =>
      log.debug("Quorum finish lock to Read")
      goto(Reading) using MessageCount(Nil, replyTo)

    case Event(Store.Succeeded(_), lockCount@LockCount(Some(_), _, _, replyTo)) if lockCount.isComplete(stores) =>
      log.debug("Quorum finish lock to Write")
      goto(Writing) using WriteCount(0, replyTo)

    case Event(Store.Succeeded(_), lockCount: LockCount) =>
      val restHead :: restTail = lockCount.rest
      restHead ! Store.Lock
      stay() using lockCount.copy(rest = restTail, locked = sender() :: lockCount.locked)

    case Event(Store.Failed(_), lockCount@LockCount(writeMessage, _, locked, replyTo)) if lockCount.hasLockedStores =>
      log.debug("Quorum locked store count: {}", locked.size)
      goto(Releasing) using ReleaseCount(locked.size, writeMessage, replyTo)

    case Event(Store.Failed(_), lockCount: LockCount) if !lockCount.hasLockedStores =>
      goto(Locking) using lockCount.copy(rest = stores.tail, locked = Nil)
  }

  when(Releasing) {
    case Event(_: Store.Result, releaseCount@ReleaseCount(_, writeMessage, replyTo)) if releaseCount.isComplete =>
      log.debug("Quorum finish release")
      goto(Locking) using LockCount(writeMessage, rest = stores.tail, locked = Nil, replyTo)

    case Event(_: Store.Result, releaseCount: ReleaseCount) =>
      stay() using releaseCount.copy(count = releaseCount.count - 1)
  }

  when(Reading) {
    case Event(message: Message, messageCount@MessageCount(messages, replyTo)) if messageCount.isComplete(stores) =>
      replyTo ! (message :: messages).maxBy(msg => msg.timestamp)
      goto(Open) using Empty

    case Event(message: Message, messageCount: MessageCount) =>
      stay() using messageCount.copy(messages = message :: messageCount.messages)
  }

  when(Writing) {
    case Event(Store.Succeeded(_), writeCount@WriteCount(_, replyTo)) if writeCount.isComplete(stores) =>
      replyTo ! Succeeded
      goto(Open) using Empty

    case Event(Store.Succeeded(_), writeCount : WriteCount) =>
      stay() using writeCount.copy(count = writeCount.count + 1)
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
