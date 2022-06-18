package zio.hdfs.utils

import java.nio.charset.StandardCharsets.UTF_8
import scala.util.Try
import scala.util.Properties
import zio.stream.ZStream
import java.io.IOException
import zio._

/**
 * Typeclass abstracting over ZIO like type constructor in order to unify some
 * APIs of both ZIO and ZStream
 */
trait ZShape[F[-_, +_, +_]]:
  def serviceWith[R: Tag, E <: Throwable, A](fn: R => F[R, E, A]): F[R, E, A]
  def blockingIO[R, A](out: F[R, Throwable, A]): F[R, IOException, A]

object ZShape:
  given ZShape[ZIO] with
    def serviceWith[R: Tag, E <: Throwable, A](fn: R => ZIO[R, E, A]): ZIO[R, E, A] = ZIO.serviceWithZIO[R](fn)
    def blockingIO[R, A](out: ZIO[R, Throwable, A]): ZIO[R, IOException, A] =
      ZIO.blocking(out).refineToOrDie[IOException]

  given ZShape[ZStream] with
    def serviceWith[R: Tag, E <: Throwable, A](fn: R => ZStream[R, E, A]): ZStream[R, E, A] =
      ZStream.serviceWithStream[R](fn)
    def blockingIO[R, A](out: ZStream[R, Throwable, A]): ZStream[R, IOException, A] =
      ZStream.blocking(out).refineToOrDie[IOException]
