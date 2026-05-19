package tethys

import cats.MonadError
import cats.syntax.flatMap._
import _root_.fs2.{Compiler, Stream}
import tethys.fs2.Cursor.Step
import tethys.fs2.JsonStreamSupport.{Collector, decode, rootError}
import tethys.readers.ReaderError

package object fs2 {
  implicit def decoderOps[A](jsonReader: JsonReader[A]): DecoderOps[A] =
    new DecoderOps[A](jsonReader)

  class DecoderOps[A](private val jsonReader: JsonReader[A]) extends AnyVal {
    def decodeFromStream[F[_], G[_]](
        stream: Stream[F, Array[Byte]],
        charset: String = "UTF-8"
    )(implicit
        compiler: Compiler[F, G],
        monadError: MonadError[G, Throwable]
    ): G[A] = {
      val decoder = new StreamingDecoder[A](Cursor(), jsonReader)

      stream
        .fold(decoder) { (state, bytes) =>
          state.feed(bytes)
          state
        }
        .map(_.finish())
        .compile
        .lastOrError
        .flatMap(result => MonadError[G, Throwable].fromEither(result))
    }
  }

  private final class StreamingDecoder[A](
      cursor: Cursor,
      jsonReader: JsonReader[A]
  ) {
    private[this] var collector: Option[Collector] = None
    private[this] var result: Option[Either[ReaderError, A]] = None
    private[this] var closed: Boolean = false

    def feed(bytes: Array[Byte]): Unit = {
      if (result.isEmpty) {
        cursor.feedInput(bytes)
        drain()
      }
    }

    def finish(): Either[ReaderError, A] = {
      try {
        result.getOrElse {
          cursor.endOfInput()
          drain()
          result.getOrElse(Left(rootError("Unexpected end of input")))
        }
      } finally {
        closeIfNeeded()
      }
    }

    private def drain(): Unit = {
      var continue = true

      while (continue && result.isEmpty) {
        cursor.next() match {
          case Step.Available(_) =>
            collector match {
              case Some(currentCollector) =>
                currentCollector.consume(cursor)
              case None =>
                collector = Some(Collector.start(cursor))
            }

            collector.foreach { currentCollector =>
              if (currentCollector.isComplete) {
                result = Some(decode(currentCollector, jsonReader))
                collector = None
              }
            }

          case Step.InputEmpty | Step.EndOfInput =>
            continue = false
        }
      }
    }

    private def closeIfNeeded(): Unit = {
      if (!closed) {
        closed = true
        cursor.close()
      }
    }
  }
}
