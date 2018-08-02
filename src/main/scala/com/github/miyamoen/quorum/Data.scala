package com.github.miyamoen.quorum

import org.joda.time.DateTime


object Data {


  def create(message: String): Data = new Data(message, DateTime.now())
}

case class Data(message: String, timestamp: DateTime)
