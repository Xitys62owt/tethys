package tethys.fs2

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.fasterxml.jackson.core.JsonParseException
import fs2.Stream
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tethys.{JsonReader, _}
import tethys.readers.ReaderError

class ParseTest extends AnyFlatSpec with Matchers {
  case class Foo(value: Int)
  object Foo {
    implicit val jsonReader: JsonReader[Foo] = JsonReader.builder
      .addField[Int]("value")
      .buildReader(Foo.apply)
  }

  object json {
    val simpleSequential =
      ("root" :: Nil) ->
        """|{
           |  "root": [
           |    {"value": 1},
           |    {"value": 2},
           |    {"value": 3},
           |    {"value": 4}
           |  ]
           |}""".stripMargin

    val nestedRepetetive =
      ("root" :: "sub" :: Nil) ->
        """|{
           |  "root": [
           |    {
           |      "sub": [
           |        {"value": 1},
           |        {"value": 2}
           |      ]
           |    },
           |    {
           |      "sub": [
           |        {"value": 3},
           |        {"value": 4}
           |      ]
           |    }
           |  ]
           |}""".stripMargin

    val nestedRepetetiveIncludingOtherTags =
      ("root" :: "sub" :: Nil) ->
        """|{
           |  "root": [
           |    {
           |      "sub": [
           |        {"value": 1},
           |        {"bar": "nope"},
           |        {"value": 2}
           |      ]
           |    },
           |    {
           |      "sub": [
           |        {"value": 3},
           |        {"value": 4}
           |      ]
           |    },
           |    {
           |      "bar": "nope"
           |    },
           |    {
           |      "sub": [
           |        {"value": 5}
           |      ]
           |    },
           |    {
           |      "sub": [
           |        {"bar": "nope"}
           |      ]
           |    }
           |  ]
           |}""".stripMargin
  }

  val expectations = Map(
    json.simpleSequential ->
      Vector(1, 2, 3, 4).map(Foo(_)).map(value => Right(value): Either[String, Foo]),
    json.nestedRepetetive ->
      Vector(1, 2, 3, 4).map(Foo(_)).map(value => Right(value): Either[String, Foo]),
    json.nestedRepetetiveIncludingOtherTags ->
      Vector(
        Right(Foo(1)),
        Left("Illegal json at '[ROOT]': Can not extract fields 'value'"),
        Right(Foo(2)),
        Right(Foo(3)),
        Right(Foo(4)),
        Right(Foo(5)),
        Left("Illegal json at '[ROOT]': Can not extract fields 'value'")
      )
  )

  private def streamBuilder(path: List[String]): Parse.OneDocument =
    path.tail.foldLeft(Parse.oneDocument(path.head))(_.inField(_))

  private def source(jsonString: String): Stream[IO, Byte] =
    Stream.emits[IO, Byte](jsonString.getBytes("UTF-8"))

  def readAtOnce(path: List[String], jsonString: String) = {
    source(jsonString).through(streamBuilder(path).everyElementAs[Foo].toFs2Stream[IO])
  }

  def readByteByByte(path: List[String], jsonString: String) = {
    source(jsonString)
      .chunkLimit(1)
      .unchunks
      .through(streamBuilder(path).everyElementAs[Foo].toFs2Stream[IO])
  }

  def normalize[T](result: Vector[Either[ReaderError, T]]) =
    result.map(_.left.map(_.getMessage))

  def assertStreamResult(stream: Stream[IO, Either[ReaderError, Foo]])(
      expects: Vector[Either[String, Foo]]
  ) = {
    normalize(stream.compile.toVector.unsafeRunSync()) shouldBe expects
  }

  "Parse" should "handle all the elements when oneDocument called with simpleSequential json" in {
    val testCase @ (path, jsonString) = json.simpleSequential

    assertStreamResult(readAtOnce(path, jsonString))(expectations(testCase))
    assertStreamResult(readByteByByte(path, jsonString))(expectations(testCase))
  }

  it should "handle all the elements when oneDocument called with nestedRepetetive json" in {
    val testCase @ (path, jsonString) = json.nestedRepetetive

    assertStreamResult(readAtOnce(path, jsonString))(expectations(testCase))
    assertStreamResult(readByteByByte(path, jsonString))(expectations(testCase))
  }

  it should "handle all the elements when oneDocument called with nestedRepetetiveIncludingOtherTags json" in {
    val testCase @ (path, jsonString) = json.nestedRepetetiveIncludingOtherTags

    assertStreamResult(readAtOnce(path, jsonString))(expectations(testCase))
    assertStreamResult(readByteByByte(path, jsonString))(expectations(testCase))
  }

  it should "fail on truncated json" in {
    val path = "root" :: Nil
    val jsonString =
      """|{
         |  "root": [
         |    {"value": 1},
         |    {"value": 2}
         |  ]
         |""".stripMargin

    val atOnceError = the[Throwable] thrownBy {
      readAtOnce(path, jsonString).compile.toVector.unsafeRunSync()
    }
    atOnceError.getMessage should include("Unexpected end")

    val byteByByteError = the[Throwable] thrownBy {
      readByteByByte(path, jsonString).compile.toVector.unsafeRunSync()
    }
    byteByByteError.getMessage should include("Unexpected end")
  }

  it should "fail on malformed json" in {
    val path = "root" :: Nil
    val jsonString =
      """|{
         |  "root": [
         |    {"value": 1},
         |    nope
         |  ]
         |}""".stripMargin

    the[JsonParseException] thrownBy {
      readAtOnce(path, jsonString).compile.toVector.unsafeRunSync()
    }

    the[JsonParseException] thrownBy {
      readByteByByte(path, jsonString).compile.toVector.unsafeRunSync()
    }
  }
}
