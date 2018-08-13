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

  case class ReadingLockCount(count: Int, address: ActorRef) extends Data

  case class WritingLockCount(count: Int, message: String, address: ActorRef) extends Data

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
      goto(ReadingLocking) using ReadingLockCount(0, sender())

    case Event(Write(message), _) =>
      goto(WritingLocking) using WritingLockCount(0, message, sender())
  }

  when(ReadingLocking) {
    case Event(Store.Succeeded, ReadingLockCount(count, address)) if count + 1 == stores.size =>
      goto(Reading) using Messages(List.empty, address)

    case Event(Store.Succeeded, ReadingLockCount(count, address)) =>
      stay() using ReadingLockCount(count + 1, address)

    case Event(Store.Failed, ReadingLockCount(_, address)) =>
      address ! Failed
      goto(Releasing) using ReleaseCount(0)
  }

  when(WritingLocking) {
    case Event(Store.Succeeded, WritingLockCount(count, message, address)) if count + 1 == stores.size =>
      goto(Writing) using WriteCount(0, address)

    case Event(Store.Succeeded, WritingLockCount(count, message, address)) =>
      stay() using WritingLockCount(count + 1, message, address)

    case Event(Store.Failed, WritingLockCount(_, _, address)) =>
      address ! Failed
      goto(Releasing) using ReleaseCount(0)
  }

  when(Releasing) {
    case Event(_: Store.Status, ReleaseCount(count)) if count + 1 == stores.size =>
      goto(Open) using Empty

    case Event(_: Store.Status, ReleaseCount(count)) =>
      stay() using ReleaseCount(count + 1)
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
      lock()
    case Open -> WritingLocking =>
      lock()

    case ReadingLocking -> Releasing =>
      release()
    case WritingLocking -> Releasing =>
      release()


    case ReadingLocking -> Reading =>
      read()

    case WritingLocking -> Writing =>
      stateData match {
        case WritingLockCount(_, message, _) =>
          write(Message.create(message))
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
