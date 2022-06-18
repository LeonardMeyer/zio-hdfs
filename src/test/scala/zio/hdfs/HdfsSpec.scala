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

object HdfsSpec extends ZIOSpec[TestEnvironment & Hdfs]:
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
    testEnvironment ++ HdfsLive.layer(conf)

  private val data     = "whatever"
  private val filePath = p"/tmp/file.txt"
  override def spec = suite("HdfsSpec")(
    test("write then read the same value") {
      for {
        _   <- Hdfs.write(filePath, data, true)
        out <- Hdfs.read(filePath).sinkAsString()
      } yield assertTrue(out == data)
    },
    test("verify if written file exists") {
      for {
        _      <- Hdfs.write(p"/tmp/exists.txt", data, true)
        exists <- Hdfs.exists(filePath)
      } yield assertTrue(exists)
    },
    test("copy file") {
      val copyPath = p"/tmp/file_copy.txt"
      for {
        bool   <- Hdfs.copy(filePath, copyPath, false)
        exists <- Hdfs.exists(filePath)
      } yield assertTrue(bool && exists)
    },
    test("move file") {
      val movePath = p"/tmp/file_move.txt"
      for {
        bool   <- Hdfs.move(filePath, movePath, false)
        exists <- Hdfs.exists(filePath)
      } yield assertTrue(bool && !exists)
    }
  )
