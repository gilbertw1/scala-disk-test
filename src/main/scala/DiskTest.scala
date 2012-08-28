import scala.util.Random
import java.io._
import java.nio.channels.FileChannel
import scala.io.Source
import net.liftweb.json._
import net.liftweb.json.Extraction._
import net.liftweb.json.Printer._

object DiskTest extends App {

	val gTimer = new Timer
	implicit val formats = DefaultFormats

	gTimer.start
	val events = EventGenerator.generateEvents(100000)
	gTimer.stop("Generating Events")

	new File("jsonCache.cache").createNewFile()

	//gTimer.start
	//writeEventsToFile("jsonCache.cache", events)
	//gTimer.stop("Writing Events to File")

	gTimer.start
	writeEventsToFileForNio("jsonCache.cache", events)
	gTimer.stop("Writing Events To JSON Cache File")

	for(i <- 1 to 10) {
		gTimer.start
		fullNioTestNewFormat()
		gTimer.stop("Full Nio Test Performed")
	}
	
	/*
	for(i <- 1 to 10) {
		gTimer.start
		emptyNioTest(events)
		gTimer.stop("Nio Read Full File")
	}
	*/

/*
	gTimer.start
	ramTest(events)
	gTimer.stop("Total in Memory")

	gTimer.start
	jsonTest(events)
	gTimer.stop("Total Lift/Json")

	gTimer.start
	emptyNioTest(events)
	gTimer.stop("Total Lift/Json Nio Read No DeSerialize")

	gTimer.start
	emptyTest(events)
	gTimer.stop("Total Lift/Json No DeSerialize")

	gTimer.start
	objectTest(events)
	gTimer.stop("Total Object")

*/

	def writeEventsToFileForNio(fileName: String, events: List[Event]) {
		val fos = new FileOutputStream(fileName, true)
		val bfos = new BufferedOutputStream (fos, 128 * 100)
		events foreach { event => 
			val bytes = Printer.compact(render(decompose(event))).getBytes 
			bfos.write(convertIntToBytes(bytes.length))
			bfos.write(bytes)
		}
	}

	def convertIntToBytes(num: Int): Array[Byte] = {
		val result = new Array[Byte](4);

		result(0) = (num >> 24).byteValue
		result(1) = (num >> 16).byteValue
		result(2) = (num >> 8).byteValue
		result(3) = (num /*>> 0*/).byteValue

		return result
	}

	def writeEventsToFile(fileName: String, events: List[Event]) {
		val fos = new FileOutputStream(fileName, true)
		val bfos = new BufferedOutputStream (fos, 128 * 100)
		events foreach { event => 
			bfos.write((Printer.compact(render(decompose(event))) + "\n").getBytes)
		}
	}

	def ramTest(events: List[Event]) {
		val timer = new Timer

		timer.start
		var memCounter = 0
		events foreach { event =>
			memCounter += 1
		}
		timer.stop("Processing in Memory Events", 1)

		println("Mem: " + memCounter)
	}

	def fullNioTestNewFormat() {
		var count = 0
		val fileName = "jsonCache.cache"
		val inChannel = new RandomAccessFile(fileName, "r").getChannel
		val buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size)

		try {
			while (buffer.hasRemaining) {
				var bcount = buffer.getInt
				//var x = 0
				//while(x < bcount) {
				//	buffer.get()
				//	x += 1
				//}

				val bytes = new Array[Byte](bcount)
				buffer.get(bytes)
				convertBytesToEvent(bytes)
				count += 1
			}
		} catch {
			case e: Exception => // Nothing
		}

		inChannel.close()

		println("NIO Count: " + count)
	}

	def convertBytesToEvent(bytes: Array[Byte]): String = {
		new String(bytes)
		//parse(new String(bytes)).extract[Event]
	}

	def fullNioTest() {

		var count = 0
		val fileName = "jsonCache.cache"
		val inChannel = new RandomAccessFile(fileName, "r").getChannel
		val buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size)

		while (buffer.hasRemaining) {

			val char = buffer.get().asInstanceOf[Char]

			//if(char == '\n') {
				//count += 1
				//parse(builder.toString).extract[Event]
			//}

			//val byte = buffer.get()

			//if(byte == 10) {
				//count += 1
				//parse(builder.toString).extract[Event]
			//}
		}

		inChannel.close()

		println("NIO Count: " + count)
	}

	def emptyNioTest(events: List[Event]) {
		val fileName = "jsonCache.cache"
		val inChannel = new RandomAccessFile(fileName, "r").getChannel
		val buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size)
		
		while (buffer.hasRemaining) {
			buffer.get().asInstanceOf[Char]
		}

		inChannel.close()
	}

	def emptyTest(events: List[Event]) {
		val timer = new Timer

		val jsonCache = new File("jsonCache.cache")
		val fStream = new FileInputStream(jsonCache)
		val source = Source.fromFile(jsonCache)

		timer.start
		var jsonCounter = 0
		source.getLines foreach { line =>
			try {
				val event = parse(line).extract[Event]
				jsonCounter += 1
			} catch {
				case e: Exception =>
					//e.printStackTrace
					//println("Line: " + line)
			}
		}
		timer.stop("Processing No DeSerialize Event File Cache", 1)

		println("Json: " + jsonCounter)
	}

	def jsonTest(events: List[Event]) {
		val timer = new Timer

		val jsonCache = new File("jsonCache.cache")
		val fStream = new FileInputStream(jsonCache)
		val source = Source.fromFile(jsonCache)

		timer.start
		var jsonCounter = 0
		source.getLines foreach { line =>
			try {
				val event = parse(line).extract[Event]
				jsonCounter += 1
			} catch {
				case e: Exception =>
					//e.printStackTrace
					//println("Line: " + line)
			}
		}
		timer.stop("Processing JSON Event File Cache", 1)

		println("Json: " + jsonCounter)
	}

	def objectTest(events: List[Event]) {
		val timer = new Timer

		val objectCache = new File("objectCache.cache")
		
		timer.start
		val fos = new FileOutputStream(objectCache)
		val out = new ObjectOutputStream(fos)
		events foreach { event =>
			out.writeObject(event)
		}
		out.close()
		timer.stop("Writing Events To Object Cache File", 1)

		timer.start
		var objectCounter = 0
		val fis = new FileInputStream(objectCache);
		val in = new ObjectInputStream(fis);
		try {
			while(true) {
				val event = in.readObject().asInstanceOf[Event]
				objectCounter += 1
			}
		} catch {
			case e: Exception =>
				//println("Exit Object Loop")
		}
		in.close();
		timer.stop("Processing Object Event File Cache", 1)

		println("Object: " + objectCounter)
	}

}

object EventGenerator {

	val random = new Random

	def generateEvents(count: Int): List[Event] = {
		var events = List[Event]()
		for(i <- 1 to count) {
			events = Event(
				ts = random.nextLong,
				values = Map (
					"value1" -> random.nextLong,
					"value2" -> random.nextLong,
					"value3" -> random.nextLong,
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
	values: Map[String,Long]
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