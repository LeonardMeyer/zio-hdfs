package zio.hdfs

import org.apache.hadoop.fs.Path
import zio.stream.ZStream
import zio.stream.ZSink
import zio.ZIO
import java.io.ByteArrayOutputStream
import zio.Scope
import java.io.IOException
import zio.Chunk
import java.nio.charset.StandardCharsets
import java.nio.charset.Charset
import org.apache.hadoop.fs.RemoteIterator

extension (sc: StringContext)
  def p(args: Any*): Path =
    val strings     = sc.parts.iterator
    val expressions = args.iterator
    var buf         = new StringBuilder(strings.next())
    while (strings.hasNext)
      buf.append(expressions.next())
      buf.append(strings.next())
    new Path(buf.mkString)

extension [R, E <: Throwable](stream: ZStream[R, E, Byte])
  def asString(charset: Charset = StandardCharsets.UTF_8): ZIO[R, E, String] =
    stream.run(ZSink.collectAll[Byte].map(chunk => new String(chunk.toArray, charset)))

given riToZStream[T]: Conversion[RemoteIterator[T], ZStream[Any, Throwable, T]] =
  ri =>
    ZStream.fromIterator {
      new Iterator[T]:
        override def hasNext: Boolean = ri.hasNext
        override def next: T          = ri.next
    }
