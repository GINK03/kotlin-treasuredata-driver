import com.treasuredata.client.*
import com.google.common.base.Function
import org.msgpack.core.MessagePack
import org.msgpack.core.MessageUnpacker
import org.msgpack.value.ArrayValue
import org.msgpack.core.MessageFormat
import com.treasuredata.client.model.*
import java.io.InputStream
import java.io.File
import kotlin.String

fun latest60DayParse(array: ArrayValue) { 
  array.mapIndexed { i, entry ->
    println("${i}, ${entry.toString()}")
  
  }
}

fun fraction(array: ArrayValue ) { 
  // ここを自由に編集して、任意の実装をしてください
  //println(array)
  latest60DayParse(array)
}


fun main(args: Array<String>) { 
  val client = TDClient.newClient();
  println("Start connecting to TreasureData Database.")
  val names:List<String> = client.listDatabases().map { db -> 
    db.getName().toString()
  }.toList()
  names.map { name -> println("There is Database of ${name}.") }
  // prestoは3倍ぐらいhiveより早いが、メモリが全然足りない40G ~ 80Gで死んでしまう
  //val jobId = client.submit(TDJobRequest.newPrestoQuery("dac_aonesync",
  val jobId = client.submit(TDJobRequest.newHiveQuery("tech_batch",
      File("kotlinDriver.sql").readText() ));
  val backOff          = ExponentialBackOff()
  val job:TDJobSummary = client.jobStatus(jobId)
  while(!client.jobStatus(jobId).getStatus().isFinished()) {
    Thread.sleep(backOff.nextWaitTimeMillis().toLong())
  }
  val jobInfo:TDJob    = client.jobInfo(jobId)
  println("log:\n ${jobInfo.getCmdOut()}")
  println("error log:\n ${jobInfo.getStdErr()}")
  val unpacker = TDHandler().unpackerHandler(client, jobId)
  println("Unpackerが呼び出されました")
  while( unpacker.hasNext() ) {
    val array = unpacker.unpackValue().asArrayValue()
    fraction(array)
  }
  println("Finished access to TresureData Database.")
  System.exit(0)
}
