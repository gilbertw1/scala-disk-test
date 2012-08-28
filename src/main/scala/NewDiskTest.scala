package newer

import scala.util.Random
import java.io._
import java.nio.channels.FileChannel
import java.nio.ByteBuffer
import scala.io.Source
import scala.collection.mutable
import net.liftweb.json._
import net.liftweb.json.Extraction._
import net.liftweb.json.Printer._

object NewDiskTest {

	def run() {
		val gTimer = new Timer
		implicit val formats = DefaultFormats

		gTimer.start
		var events = EventGenerator.generateEvents(100000)
		gTimer.stop("Generating Events")

		new File("bffCache.cache").createNewFile()

		gTimer.start
		writeEventsToFileForNio("bffCache.cache", events)
		gTimer.stop("Writing Events To BFF Cache File")

		events = Nil

		for(i <- 1 to 10) {
			gTimer.start
			fullNioTestNewFormat()
			gTimer.stop("Full Nio Test Performed")
		}
	}

	def writeEventsToFileForNio(fileName: String, events: List[Event]) {
		val fos = new FileOutputStream(fileName, true)
		val bfos = new BufferedOutputStream (fos, 128 * 100)
		events foreach { event => 
			bfos.write(convertEventToBytes(event))
		}
	}

	def convertShortToBytes(num: Short): Array[Byte] = {
		//val result = new Array[Byte](2)
		//result(0) = (short & 0xff).byteValue
		//result(1) = ((short >> 8) & 0xff).byteValue
		//result
		ByteBuffer.allocate(2).putShort(num).array()
	}

	def convertIntToBytes(num: Int): Array[Byte] = {
		//val result = new Array[Byte](4)
		//result(0) = (num >> 24).byteValue
		//result(1) = (num >> 16).byteValue
		//result(2) = (num >> 8).byteValue
		//result(3) = (num /*>> 0*/).byteValue
		//result
		ByteBuffer.allocate(4).putInt(num).array()
	}

	def convertLongToBytes(num: Long): Array[Byte] = {
		ByteBuffer.allocate(8).putLong(num).array()
	}

	def convertDoubleToBytes(num: Double): Array[Byte] = {
		convertLongToBytes(java.lang.Double.doubleToLongBits(num))
	}

	def fullNioTestNewFormat() {
		val fileName = "bffCache.cache"
		val inChannel = new RandomAccessFile(fileName, "r").getChannel
		val buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size)

		while (buffer.hasRemaining) {
			val event = readEventFromBuffer(buffer)
		}

		inChannel.close()
	}

	def readEventFromBuffer(buffer: ByteBuffer): Event = {
		val ts = buffer.getLong
		val strValues = mutable.Map[String,String]()
		val dblValues = mutable.Map[String,Double]()

		val strCount = buffer.getShort
		var cnt = 0
		while(cnt < strCount) {
			strValues += (readStringValue(buffer) -> readStringValue(buffer))
			cnt += 1
		}

		val dblCount = buffer.getShort
		cnt = 0
		while(cnt < dblCount) {
			dblValues += (readStringValue(buffer) -> (readDoubleValue(buffer)))
			cnt += 1
		}

		Event(ts, strValues, dblValues)
	}

	def readStringValue(buffer: ByteBuffer): String = {
		val strSize = buffer.get
		val byteArr = new Array[Byte](strSize.intValue)
		buffer.get(byteArr)
		new String(byteArr)
	}

	def readDoubleValue(buffer: ByteBuffer): Double = {
		java.lang.Double.longBitsToDouble(buffer.getLong)
	}

	def convertEventToBytes(event: Event): Array[Byte] = {
		val bytes = mutable.ArrayBuffer[Byte]()

		bytes ++= convertLongToBytes(event.ts)
		bytes ++= convertShortToBytes(event.strValues.size.shortValue)
		event.strValues foreach { case (key, value) =>
			bytes += key.size.byteValue
			bytes ++= key.getBytes
			bytes += value.size.byteValue
			bytes ++= value.getBytes
		}
		bytes ++= convertShortToBytes(event.dblValues.size.shortValue)
		event.dblValues foreach { case (key, value) =>
			bytes += key.size.byteValue
			bytes ++= key.getBytes
			bytes ++= convertDoubleToBytes(value)
		}

		bytes.toArray
	}
}

object EventGenerator {

	val random = new Random

	def generateEvents(count: Int): List[Event] = {
		var events = List[Event]()
		for(i <- 1 to count) {
			events = Event(
				ts = random.nextLong,
				strValues = mutable.Map (
					"strValue1" -> "facility-1",
					"strValue2" -> "sparc"
				),
				dblValues = mutable.Map (
					"value1" -> random.nextLong,
					"value2" -> random.nextLong,
					"value3" -> random.nextLong,
					"value4" -> random.nextLong,
					"value5" -> random.nextLong,
					"value6" -> random.nextLong,
					"value7" -> random.nextLong,
					"value8" -> random.nextLong,
					"value9" -> random.nextLong,
					"value10" -> random.nextLong
				)
			) :: events
		}
		events
	}

}

case class Event (
	ts: Long,
	strValues: mutable.Map[String,String],
	dblValues: mutable.Map[String,Double]
)

object Timer {
	var printTimings = true
}

class Timer {
	var sTime = 0L

	def start() {
		sTime = System.currentTimeMillis
	}

	def stop(msg: String) {
		stop(msg, 0)
	}

	def stop(msg: String, nesting: Int) {
		val runTime = System.currentTimeMillis - sTime
		if(Timer.printTimings) {
			println(("  " * nesting) + msg + ": " + runTime)
		}
	}
}