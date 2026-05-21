package tethys.fs2

import java.nio.file.{Path, Paths}

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import fs2.io.file.Files
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tethys.{JsonReader, _}

class LargeJsonTest extends AnyFlatSpec with Matchers {
  case class Actor(login: String)
  object Actor {
    implicit val jsonReader: JsonReader[Actor] = JsonReader.builder
      .addField[String]("login")
      .buildReader(Actor.apply)
  }

  case class Repo(name: String)
  object Repo {
    implicit val jsonReader: JsonReader[Repo] = JsonReader.builder
      .addField[String]("name")
      .buildReader(Repo.apply)
  }

  case class Event(
      id: String,
      eventType: String,
      public: Boolean,
      createdAt: String,
      actor: Actor,
      repo: Repo
  )
  object Event {
    implicit val jsonReader: JsonReader[Event] = JsonReader.builder
      .addField[String]("id")
      .addField[String]("type")
      .addField[Boolean]("public")
      .addField[String]("created_at")
      .addField[Actor]("actor")
      .addField[Repo]("repo")
      .buildReader(Event.apply)
  }

  private val resourcePath: Path =
    Paths.get(getClass.getResource("/tethys/fs2/large-file.json").toURI)

  private def byteStream(chunkSize: Int = 64 * 1024): Stream[IO, Byte] =
    Files[IO].readAll(resourcePath, chunkSize)

  private def byteArrayStream(chunkSize: Int = 64 * 1024): Stream[IO, Array[Byte]] =
    byteStream(chunkSize).chunks.map(_.toArray)

  "Fs2 integration" should "decode a large json document with decodeFromStream" in {
    val events = JsonReader[List[Event]].decodeFromStream(byteArrayStream()).unsafeRunSync()

    events.length shouldBe 11351
    events.head shouldBe Event(
      "2489651045",
      "CreateEvent",
      public = true,
      "2015-01-01T15:00:00Z",
      Actor("petroav"),
      Repo("petroav/6.828")
    )
    events.last shouldBe Event(
      "2489678844",
      "IssuesEvent",
      public = true,
      "2015-01-01T15:59:59Z",
      Actor("No-CQRT"),
      Repo("No-CQRT/GooGuns")
    )
  }

  it should "decode elements from a large json document with Parse.toFs2Stream" in {
    val events = Parse.oneDocument.everyElementAs[Event].toFs2Stream[IO](byteStream()).compile.toVector.unsafeRunSync()

    events.length shouldBe 11351
    events.head shouldBe Right(
      Event(
        "2489651045",
        "CreateEvent",
        public = true,
        "2015-01-01T15:00:00Z",
        Actor("petroav"),
        Repo("petroav/6.828")
      )
    )
    events.last shouldBe Right(
      Event(
        "2489678844",
        "IssuesEvent",
        public = true,
        "2015-01-01T15:59:59Z",
        Actor("No-CQRT"),
        Repo("No-CQRT/GooGuns")
      )
    )
  }
}
