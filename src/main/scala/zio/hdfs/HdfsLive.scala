package zio.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.*
import zio.*
import scala.util.Using
import zio.hdfs.utils.*
import zio.stream.Stream
import java.io.IOException
import zio.stream.ZStream
import org.apache.hadoop.fs.permission.FsPermission

case class HdfsLive private (fileSystem: FileSystem) extends Hdfs:

  override def withFs[T, E <: Throwable, F[-_, +E, +_]](fn: FileSystem => F[Any, E, T])(using
    z: ZShape[F]
  ): F[Any, IOException, T] =
    z.blockingIO(fn(fileSystem))

  override def copy(srcPath: Path, destPath: Path, overwrite: Boolean): IO[IOException, Boolean] =
    withFs { fs =>
      ZIO.logDebug(s"Trying to copy '$srcPath' to '$destPath'...") *>
        ZIO.attempt(FileUtil.copy(fs, srcPath, fs, destPath, false, overwrite, fs.getConf))
    }
  override def move(srcPath: Path, destPath: Path, overwrite: Boolean): IO[IOException, Boolean] =
    withFs { fs =>
      ZIO.logDebug(s"Trying to move '$srcPath' to '$destPath'...") *>
        ZIO.attempt(FileUtil.copy(fs, srcPath, fs, destPath, true, overwrite, fs.getConf))
    }

  override def exists(path: Path): IO[IOException, Boolean] =
    withFs(fs => ZIO.logDebug(s"Checking if '$path' exists...") *> ZIO.attempt(fs.exists(path)))

  override def write[T: AsByteArray](destPath: Path, data: T, overwrite: Boolean): IO[IOException, Unit] =
    withFs { fs =>
      ZIO.logDebug(s"Trying to write '$data' to '$destPath'...") *>
        ZIO
          .attempt(fs.create(destPath, overwrite))
          .acquireReleaseWithAuto(dos =>
            ZIO.attempt {
              dos.write(data.asByteArray)
              dos.flush()
            }
          )
    }

  override def mkdirs(path: Path, fsPermission: FsPermission): IO[IOException, Unit] =
    withFs(fs =>
      ZIO.logDebug(s"Creating directories '$path' with permissions '$fsPermission'...") *> ZIO.attempt(
        fs.mkdirs(path, fsPermission)
      )
    )

  override def read(path: Path): Stream[IOException, Byte] =
    withFs { fs =>
      ZStream.logDebug(s"Openining stream for file '$path'") *>
        ZStream.fromInputStreamZIO(ZIO.attempt(fs.open(path)).refineToOrDie[IOException])
    }
  override def streamStatus(path: Path): Stream[IOException, FileStatus] =
    withFs(fs => ZStream.logDebug(s"Openining stream for file '$path'") *> fs.listStatusIterator(path))
  override def streamLocatedStatus(path: Path, recursive: Boolean): Stream[IOException, LocatedFileStatus] =
    withFs(fs => ZStream.logDebug(s"Openining stream for file '$path'") *> fs.listFiles(path, recursive))

end HdfsLive

object HdfsLive:
  def layer(config: => Configuration): ZLayer[Any, Throwable, HdfsLive] = ZLayer.scoped {
    for {
      fileSystem <- ZIO.fromAutoCloseable(ZIO.attempt(FileSystem.get(config)))
    } yield HdfsLive(fileSystem)
  }
end HdfsLive
