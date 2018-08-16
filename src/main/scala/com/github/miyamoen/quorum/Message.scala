package com.github.miyamoen.quorum

import org.joda.time.DateTime

case class Message(message: String, timestamp: DateTime = DateTime.now())