package com.github.miyamoen.quorum

import org.joda.time.DateTime


object Message {


  def create(message: String): Message = new Message(message, DateTime.now())
}

case class Message(message: String, timestamp: DateTime)
