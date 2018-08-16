package com.github.miyamoen.quorum

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.{Failure, Success}

object Main extends App {
  implicit val timeout: Timeout = Timeout(Duration(30, SECONDS))
  val system = ActorSystem("QuorumSystem")
  val quorumSystem = system.actorOf(QuorumSystem.props())

  val res = quorumSystem ? Quorum.Read
  res.map {
    case Quorum.Failed =>
      None
    case message: Message =>
      Some(message)
  } onComplete {
    case Success(Some(message)) =>
      println("Read0 Succeeded! : {}", message)
    case Success(None) =>
      println("Read0 Failed!")
    case Failure(ex) =>
      println("Read0 Failed! : {}", ex)
  }

  val writeRes = quorumSystem ? Quorum.Write("更新")
  writeRes.mapTo[Quorum.Status] onComplete {
    case Success(Quorum.Succeeded) =>
      println("Write Succeeded!")
    case Success(Quorum.Failed) =>
      println("Write Failed!")
    case Failure(ex) =>
      println("Write Failed! : {}", ex)
  }

    val res1 = quorumSystem ? Quorum.Read
    res1.map {
      case Quorum.Failed =>
        None
      case message: Message =>
        Some(message)
    } onComplete {
      case Success(Some(message)) =>
        println("Read1 Succeeded! : {}", message)
      case Success(None) =>
        println("Read1 Failed!")
      case Failure(ex) =>
        println("Read1 Failed! : {}", ex)
    }

  Future.sequence(List(res,res1, writeRes)) onComplete {
    case _ =>
      system.terminate()
  }
}

