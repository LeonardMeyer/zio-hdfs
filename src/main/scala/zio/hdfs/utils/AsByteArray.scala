package zio.hdfs.utils

import java.nio.charset.StandardCharsets.UTF_8
import scala.util.Try
import scala.util.Properties

trait AsByteArray[T]:
  def toByteArray(t: T): Array[Byte]
  extension (t: T) def asByteArray: Array[Byte] = toByteArray(t)

object AsByteArray:
  given AsByteArray[String] = _.getBytes(UTF_8)

  given AsByteArray[Array[Byte]] = identity(_)

  given [T: AsByteArray, F[T] <: Iterable[T]]: AsByteArray[F[T]] =
    dataIter =>
      val sep = Properties.lineSeparator
      dataIter.map(_.asByteArray).reduce(_ ++ sep.getBytes(UTF_8) ++ _).toArray
