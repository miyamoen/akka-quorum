package com.github.miyamoen.quorum

class DataSpec extends BaseSpec {
  "Data.create" should {
    "create data with an message and timestamp" in {
      val someMessage = "some message"
      val data = Data.create(someMessage)
      assert(data.message == someMessage)
    }
  }
}
