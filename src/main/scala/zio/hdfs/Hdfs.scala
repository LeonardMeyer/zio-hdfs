package zio.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.*
import zio.*
import scala.util.Using
import zio.hdfs.utils.*
import zio.stream.Stream
import java.io.IOException
import org.apache.hadoop.fs.RemoteIterator
import zio.stream.ZStream
import org.apache.hadoop.fs.permission.FsPermission

/**
 * Hdfs ZIO service. Use the exposed Hdfs methods in your code and provide using
 * the layer method
 */

trait Hdfs:
  def withFs[T, E <: Throwable, F[-_, +E, +_]](fn: FileSystem => F[Any, E, T])(using
    z: ZShape[F]
  ): F[Any, IOException, T]
  def copy(srcPath: Path, destPath: Path, overwrite: Boolean): IO[IOException, Boolean]
  def move(srcPath: Path, destPath: Path, overwrite: Boolean): IO[IOException, Boolean]
  def exists(path: Path): IO[IOException, Boolean]
  def write[T: AsByteArray](destPath: Path, data: T, overwrite: Boolean): IO[IOException, Unit]
  def mkdirs(path: Path, fsPermission: FsPermission): IO[IOException, Unit]
  def read(path: Path): Stream[IOException, Byte]
  def streamStatus(path: Path): Stream[IOException, FileStatus]
  def streamLocatedStatus(path: Path, recursive: Boolean): Stream[IOException, LocatedFileStatus]
end Hdfs

object Hdfs:
  def withFs[T, E <: Throwable, F[-_, +E, +_]](fn: FileSystem => F[Any, E, T])(using
    z: ZShape[F]
  ): F[Hdfs, IOException, T] =
    z.serviceWith(_.withFs(fn))

  /**
   * @param srcPath
   * @param destPath
   * @param overwrite
   * @return
   */
  def copy(srcPath: Path, destPath: Path, overwrite: Boolean = false): ZIO[Hdfs, IOException, Boolean] =
    ZIO.serviceWithZIO[Hdfs](_.copy(srcPath, destPath, overwrite))

  def move(srcPath: Path, destPath: Path, overwrite: Boolean = false): ZIO[Hdfs, IOException, Boolean] =
    ZIO.serviceWithZIO[Hdfs](_.move(srcPath, destPath, overwrite))

  def exists(path: Path): ZIO[Hdfs, IOException, Boolean] =
    ZIO.serviceWithZIO[Hdfs](_.exists(path))

  def write[T: AsByteArray](destPath: Path, data: T, overwrite: Boolean = false): ZIO[Hdfs, IOException, Unit] =
    ZIO.serviceWithZIO[Hdfs](_.write(destPath, data, overwrite))

  def mkdirs(path: Path, fsPermission: FsPermission = FsPermission.getDirDefault()): ZIO[Hdfs, IOException, Unit] =
    ZIO.serviceWithZIO[Hdfs](_.mkdirs(path, fsPermission))

  def read(path: Path): ZStream[Hdfs, IOException, Byte] =
    ZStream.serviceWithStream[Hdfs](_.read(path))

  def streamStatus(path: Path): ZStream[Hdfs, IOException, FileStatus] =
    ZStream.serviceWithStream[Hdfs](_.streamStatus(path))

  def streamLocatedStatus(path: Path, recursive: Boolean): ZStream[Hdfs, IOException, LocatedFileStatus] =
    ZStream.serviceWithStream[Hdfs](_.streamLocatedStatus(path, recursive))

end Hdfs
