import org.apache.spark.sql.types.{StructType, StructField, DoubleType, LongType, ArrayType, BooleanType, DataType}
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, MutableAggregationBuffer}
import org.apache.spark.sql.Row
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Breaks._

class ApproxQuantile (
  probabilities: Array[Double], 
  relativeError: Double, 
  headSize: Integer = ApproxQuantile.defaultHeadSize, 
  compressThreshold: Integer = ApproxQuantile.defaultCompressThreshold) extends UserDefinedAggregateFunction {
  
  import ApproxQuantile._

  override def inputSchema: StructType = StructType (
    StructField("value", DoubleType) :: Nil)

  private def statsSchema: StructType = StructType (
    StructField("value", DoubleType) :: 
    StructField("g", LongType) :: 
    StructField("delta", LongType) :: Nil)

  override def bufferSchema: StructType = StructType (
    StructField("sampled", ArrayType(statsSchema)) ::
    StructField("count", LongType) ::
    StructField("compressed", BooleanType) ::
    StructField("headSampled", ArrayType(DoubleType)) :: Nil)

  override def dataType: DataType = ArrayType(DoubleType)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(Buffer.FieldIndex.sampled) = Seq.empty[Row]
    buffer(Buffer.FieldIndex.count) = 0L
    buffer(Buffer.FieldIndex.compressed) = false
    buffer(Buffer.FieldIndex.headSampled) = Seq.empty[Double]
  }

  private def withHeadBufferInserted(buffer: Buffer): Buffer = {
    if (buffer.headSampled.isEmpty) {
      return buffer
    }
    var currentCount = buffer.count
    val sorted = buffer.headSampled.toArray.sorted
    val newSamples: ArrayBuffer[Statistics] = new ArrayBuffer[Statistics]()
    var sampleIdx = 0
    var opsIdx: Integer = 0
    while (opsIdx < sorted.length) {
      val currentSample = sorted(opsIdx)
      while (sampleIdx < buffer.sampled.length && buffer.sampled(sampleIdx).value <= currentSample) {
        newSamples += buffer.sampled(sampleIdx)
        sampleIdx += 1
      }
      currentCount += 1
      val delta = if (newSamples.isEmpty || (sampleIdx == buffer.sampled.length && opsIdx == sorted.length - 1)) {
        0
      } else {
        math.floor(2 * relativeError * currentCount).toLong
      }
      newSamples += Statistics.apply(currentSample, 1, delta)
      opsIdx += 1
    }
    while (sampleIdx < buffer.sampled.length) {
      newSamples += buffer.sampled(sampleIdx)
      sampleIdx += 1
    }
    return Buffer.apply(newSamples.toSeq, currentCount)
  }

  private def compressed(buffer: Buffer): Buffer = {
    val inserted = withHeadBufferInserted(buffer)
    assert(inserted.headSampled.isEmpty)
    assert(inserted.count == buffer.count + buffer.headSampled.size)
    val compressed = samplesCompressed(inserted.sampled.toIndexedSeq, 2 * relativeError * inserted.count)
    return Buffer.apply(compressed, inserted.count, true)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val currBuf = Buffer.fromRow(buffer)
    val newSamp = input.getAs[Double](inputSchema.fieldIndex("value"))
    val bufWithNewSamp = Buffer.apply(currBuf.sampled, currBuf.count, false, currBuf.headSampled :+ newSamp)
    if (bufWithNewSamp.headSampled.size >= headSize) {
      val bufWithHeadBuf = withHeadBufferInserted(bufWithNewSamp)
      if (bufWithHeadBuf.sampled.length >= compressThreshold) {
        applyBuffer(buffer, compressed(bufWithHeadBuf))
      } else {
        applyBuffer(buffer, bufWithHeadBuf)
      }
    } else {
      applyBuffer(buffer, bufWithNewSamp)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val compressedBuf1 = {
      val currBuf1 = Buffer.fromRow(buffer1)
      if (currBuf1.headSampled.isEmpty) currBuf1 else compressed(currBuf1)
    }
    require(compressedBuf1.headSampled.isEmpty, "Current buffer needs to be compressed before merge")
    val compressedBuf2 = {
      val currBuf2 = Buffer.fromRow(buffer2)
      if (currBuf2.headSampled.isEmpty) currBuf2 else compressed(currBuf2)
    }
    require(compressedBuf2.headSampled.isEmpty, "Other buffer needs to be compressed before merge")
    if (compressedBuf2.count == 0) {
      applyBuffer(buffer1, compressedBuf1)
    } else if (compressedBuf1.count == 0) {
      applyBuffer(buffer1, compressedBuf2)
    } else {
      val mergedSampled = new ArrayBuffer[Statistics]()
      val mergedCount = compressedBuf1.count + compressedBuf2.count
      val additionalDelta1 = math.floor(2 * relativeError * compressedBuf2.count).toLong
      val additionalDelta2 = math.floor(2 * relativeError * compressedBuf1.count).toLong
      var idx1 = 0
      var idx2 = 0
      while (idx1 < compressedBuf1.sampled.length && idx2 < compressedBuf2.sampled.length) {
        val sample1 = compressedBuf1.sampled(idx1)
        val sample2 = compressedBuf2.sampled(idx2)
        val (nextSample, additionalDelta) = if (sample1.value < sample2.value) {
          idx1 += 1
          (sample1, if (idx2 > 0) additionalDelta1 else 0)
        } else {
          idx2 += 1
          (sample2, if (idx1 > 0) additionalDelta2 else 0)
        }
        mergedSampled += nextSample.copy(delta = nextSample.delta + additionalDelta)
      }
      while (idx1 < compressedBuf1.sampled.length) {
        mergedSampled += compressedBuf1.sampled(idx1)
        idx1 += 1
      }
      while (idx2 < compressedBuf2.sampled.length) {
        mergedSampled += compressedBuf2.sampled(idx2)
        idx2 += 1
      }
      val compressed = samplesCompressed(mergedSampled, 2 * relativeError * mergedCount)
      applyBuffer(buffer1, Buffer.apply(compressed, mergedCount, true))
    }
  }

  override def evaluate(buffer: Row): Any = {
    val currBuf = Buffer.fromRow(buffer)
    val compressedBuf = if (currBuf.headSampled.isEmpty) {
      currBuf
    } else {
      compressed(currBuf)
    }
    if (compressedBuf.sampled.isEmpty) {
      return 0.0
    }
    require(compressedBuf.headSampled.isEmpty, "Cannot operate on an uncompressed summary")
    probabilities.map { quantile => 
      require(quantile >= 0 && quantile <= 1.0, "Quantile should be in the range [0.0, 1.0]")
      var res = compressedBuf.sampled.last.value
      if (quantile <= relativeError) {
        res = compressedBuf.sampled.head.value
      } else if (quantile >= 1 - relativeError) {
        res = compressedBuf.sampled.last.value
      } else {
        val rank = math.ceil(quantile * compressedBuf.count).toLong
        val targetError = relativeError * compressedBuf.count
        val sampled = compressedBuf.sampled.toIndexedSeq
        var minRank = 0L
        var i = 0
        breakable { while (i < compressedBuf.sampled.length - 1) {
          val curSample = sampled(i)
          minRank += curSample.g
          val maxRank = minRank + curSample.delta
          if (maxRank - targetError <= rank && rank <= minRank + targetError) {
            res = curSample.value
            break
          }
          i += 1
        } }
      }
      res
    }.toSeq
  }
}

object ApproxQuantile {
  
  val defaultHeadSize: Integer = 50000
  val defaultCompressThreshold: Integer = 10000

  case class Statistics(value: Double, g: Long, delta: Long) {
    def row: Row = Row.apply(
      value, 
      g, 
      delta)
  }

  object Statistics {
    def fromRow(statistics: Row): Statistics = {
      Statistics.apply(
        statistics.getAs[Double](FieldIndex.value),
        statistics.getAs[Long](FieldIndex.g),
        statistics.getAs[Long](FieldIndex.delta))
    }
    object FieldIndex {
      val value = 0
      val g = 1
      val delta = 2
    }
  }

  case class Buffer(sampled: Seq[Statistics], count: Long, compressed: Boolean = false, headSampled: Seq[Double] = Seq.empty[Double]) {
    def row: Row = Row.apply(
      sampled.map((sample: Statistics) => sample.row),
      count, 
      compressed, 
      headSampled)
  }

  object Buffer {
    def fromRow(buffer: Row): Buffer = {
      Buffer.apply(
        buffer.getSeq[Row](FieldIndex.sampled).map(row => Statistics.fromRow(row)),
        buffer.getAs[Long](FieldIndex.count),
        buffer.getAs[Boolean](FieldIndex.compressed),
        buffer.getSeq[Double](FieldIndex.headSampled))
    }
    object FieldIndex {
      val sampled = 0
      val count = 1
      val compressed = 2
      val headSampled = 3
    }  
  }

  private def applyBuffer(buffer1: MutableAggregationBuffer, buffer2: Buffer): Unit = {
    buffer1(Buffer.FieldIndex.sampled) = buffer2.sampled.map(sample => sample.row)
    buffer1(Buffer.FieldIndex.count) = buffer2.count
    buffer1(Buffer.FieldIndex.compressed) = buffer2.compressed
    buffer1(Buffer.FieldIndex.headSampled) = buffer2.headSampled
  }

  private def samplesCompressed(currentSamples: IndexedSeq[Statistics], mergeThreshold: Double): Seq[Statistics] = {
    if (currentSamples.isEmpty) {
      return Seq.empty[Statistics]
    }
    val res = ListBuffer.empty[Statistics]
    var head = currentSamples.last
    var i = currentSamples.size - 2
    while (i >= 1) {
      val sample1 = currentSamples(i)
      if (sample1.g + head.g + head.delta < mergeThreshold) {
        head = head.copy(g = head.g + sample1.g)
      } else {
        res.prepend(head)
        head = sample1
      }
      i -= 1
    }
    res.prepend(head)
    val currHead = currentSamples.head
    if (currHead.value <= head.value && currentSamples.length > 1) {
      res.prepend(currentSamples.head)
    }
    return res.toSeq
  }
}