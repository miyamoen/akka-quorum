package com.github.miyamoen.quorum

import akka.actor.{Actor, ActorRef, LoggingFSM, Props}
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

  sealed trait LockCount extends Data {
    val rest: List[ActorRef]
    val locked: List[ActorRef]
    val replyTo: ActorRef

    def update(rest: List[ActorRef], locked: List[ActorRef]): LockCount

    def isComplete: Boolean = rest.size == 1

    def hasLockedStores: Boolean = locked.nonEmpty

    def lock()(implicit sender: ActorRef): Unit = rest.head ! Store.Lock

    def release()(implicit sender: ActorRef): Unit = locked.foreach(store => store ! Store.Release)

    def nextLockCount()(implicit sender: ActorRef): LockCount = {
      val lockedStore :: restTail = rest
      restTail.head ! Store.Lock
      update(rest = restTail, locked = lockedStore :: locked)
    }
  }

  case class LockCountForRead(rest: List[ActorRef], locked: List[ActorRef], replyTo: ActorRef) extends LockCount {
    override def update(rest: List[ActorRef], locked: List[ActorRef]): LockCountForRead = copy(rest = rest, locked = locked)
  }

  case class LockCountForWrite(writeMessage: String, rest: List[ActorRef], locked: List[ActorRef], replyTo: ActorRef) extends LockCount {
    override def update(rest: List[ActorRef], locked: List[ActorRef]): LockCountForWrite = copy(rest = rest, locked = locked)
  }

  sealed trait ReleaseCount extends Data {
    val count: Int
    val replyTo: ActorRef

    def update(count: Int): ReleaseCount

    def isComplete: Boolean = count - 1 == 0

  }

  case class ReleaseCountForWrite(writeMessage: String, count: Int, replyTo: ActorRef) extends ReleaseCount {
    override def update(count: Int): ReleaseCountForWrite = copy(count = count)
  }

  case class ReleaseCountForRead(count: Int, replyTo: ActorRef) extends ReleaseCount {
    override def update(count: Int): ReleaseCountForRead = copy(count = count)
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
      goto(Locking) using LockCountForRead(rest = stores, locked = Nil, sender())

    case Event(Write(message), _) =>
      goto(Locking) using LockCountForWrite(writeMessage = message, rest = stores, locked = Nil, sender())
  }
  when(Locking) {
    case Event(Store.Succeeded(_), lockCount: LockCountForRead) if lockCount.isComplete =>
      log.debug("Quorum finish lock to Read")
      goto(Reading) using MessageCount(Nil, lockCount.replyTo)

    case Event(Store.Succeeded(_), lockCount: LockCountForWrite) if lockCount.isComplete =>
      log.debug("Quorum finish lock to Write")
      goto(Writing) using WriteCount(0, lockCount.replyTo)

    case Event(Store.Succeeded(_), lockCount: LockCount) =>
      stay() using lockCount.nextLockCount()

    case Event(Store.Failed(_), lockCount: LockCountForRead) if lockCount.hasLockedStores =>
      log.debug("Quorum locked store count: {}", lockCount.locked.size)
      goto(Releasing) using ReleaseCountForRead(lockCount.locked.size, lockCount.replyTo)

    case Event(Store.Failed(_), lockCount: LockCountForWrite) if lockCount.hasLockedStores =>
      log.debug("Quorum locked store count: {}", lockCount.locked.size)
      goto(Releasing) using ReleaseCountForWrite(lockCount.writeMessage, lockCount.locked.size, lockCount.replyTo)

    case Event(Store.Failed(_), lockCount: LockCount) if !lockCount.hasLockedStores =>
      goto(Locking) using lockCount.update(rest = stores, locked = Nil)
  }

  when(Releasing) {
    case Event(_: Store.Result, releaseCount: ReleaseCountForRead) if releaseCount.isComplete =>
      log.debug("Quorum finish release for Read")
      goto(Locking) using LockCountForRead(rest = stores, locked = Nil, releaseCount.replyTo)

    case Event(_: Store.Result, releaseCount: ReleaseCountForWrite) if releaseCount.isComplete =>
      log.debug("Quorum finish release for Write")
      goto(Locking) using LockCountForWrite(writeMessage = releaseCount.writeMessage, rest = stores, locked = Nil, replyTo = releaseCount.replyTo)

    case Event(_: Store.Result, releaseCount: ReleaseCount) =>
      stay() using releaseCount.update(count = releaseCount.count - 1)
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

    case Event(Store.Succeeded(_), writeCount: WriteCount) =>
      stay() using writeCount.copy(count = writeCount.count + 1)
  }

  onTransition {
    case state -> Locking =>
      log.debug("Quorum Lock from {} state", state)
      nextStateData match {
        case lockCount: LockCount =>
          lockCount.lock()
      }

    case Locking -> Releasing =>
      stateData match {
        case lockCount: LockCount =>
          log.debug("Quorum Lock Release")
          lockCount.release()
      }

    case Locking -> Reading =>
      log.debug("Quorum Read")
      read()

    case Locking -> Writing =>
      log.debug("Quorum Write")
      stateData match {
        case LockCountForWrite(writeMessage, _, _, _) =>
          write(Message(writeMessage))
      }
  }

  private def read(): Unit = {
    stores.foreach(store => store ! Store.Read)
  }

  private def write(message: Message): Unit = {
    stores.foreach(store => store ! Store.Write(message))
  }

}
