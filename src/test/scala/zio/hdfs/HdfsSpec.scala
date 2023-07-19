package zio.hdfs

import zio.test._
import zio.test.Assertion._
import zio._

import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.test.PathUtils
import org.apache.hadoop.fs.Path

import java.io.IOException
import zio.stream.ZStream
import zio.stream.ZSink

import java.nio.file.Paths

object HdfsSpec extends ZIOSpec[Hdfs]:
  override val bootstrap =
    val baseDir = new File(PathUtils.getTestDir(getClass()), "miniHDFS")
    val conf    = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath())
    conf.setBoolean("dfs.webhdfs.enabled", true)
    new MiniDFSCluster.Builder(conf)
      .nameNodePort(9000)
      .manageNameDfsDirs(true)
      .manageDataDfsDirs(true)
      .format(true)
      .build()
      .waitClusterUp()
    HdfsLive.layer(conf)

  private val data = "whatever"
  override def spec = suite("HdfsSpec")(
    test("write then read the same value") {
      val filePath = p"/tmp/file.txt"
      for {
        _   <- Hdfs.write(filePath, data, true)
        out <- Hdfs.read(filePath).asString()
      } yield assertTrue(out == data)
    },
    test("verify if written file exists") {
      val newFile = p"/tmp/exists.txt"
      for {
        _      <- Hdfs.write(newFile, data, true)
        exists <- Hdfs.exists(newFile)
      } yield assertTrue(exists)
    },
    test("copy file") {
      val filePath = p"/tmp/file2.txt"
      val copyPath = p"/tmp/file2_copy.txt"
      for {
        _      <- Hdfs.write(filePath, data, true)
        bool   <- Hdfs.copy(filePath, copyPath, true)
        exists <- Hdfs.exists(filePath)
      } yield assertTrue(bool && exists)
    },
    test("move file") {
      val filePath = p"/tmp/file3.txt"
      val movePath = p"/tmp/file_move3.txt"
      for {
        _      <- Hdfs.write(filePath, data, true)
        bool   <- Hdfs.move(filePath, movePath, false)
        exists <- Hdfs.exists(filePath)
      } yield assertTrue(bool && !exists)
    }
  )
