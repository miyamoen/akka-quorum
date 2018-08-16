package com.github.miyamoen.quorum

class MessageSpec extends BaseSpec {
  "Message.create" should {
    "create data with an message and timestamp" in {
      val someMessage = "some message"
      val data = Message(someMessage)
      assert(data.message == someMessage)
    }
  }
}
