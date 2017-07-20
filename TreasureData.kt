import com.treasuredata.client.*
import com.google.common.base.Function
import org.msgpack.core.MessagePack
import org.msgpack.core.MessageUnpacker
import org.msgpack.value.ArrayValue
import org.msgpack.core.MessageFormat
import com.treasuredata.client.model.*
import java.io.InputStream
import java.io.File
import java.io.PrintWriter

import kotlin.String

import com.google.gson.Gson
import com.google.gson.GsonBuilder


val gson = Gson()
val pwriter = PrintWriter("download.json")
fun f1( array: ArrayValue ) { 
  val arr = array.toList().map{ x -> x.toString()}
  val tuuid = arr[6]
  when { 
    tuuid != "null" -> { 
      //println( tuuid )
      val json = gson.toJson( arr )
      pwriter.append(json + "\n")
    }
  }
}
fun close() {
  pwriter.close()
}

fun <T> printerr(t: T) {
  System.`err`.println(t)
}

fun main(args: Array<String>) { 
  val client = TDClient.newClient()
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
  for (counter in (0..10000000)) {
    if( client.jobStatus(jobId).getStatus().isFinished() ) break
    printerr("waiting result... now iter ${counter}...")
    Thread.sleep(500)
  }
  val jobInfo:TDJob    = client.jobInfo(jobId)
  println("log:\n ${jobInfo.getCmdOut()}")
  println("error log:\n ${jobInfo.getStdErr()}")
  val unpacker = TDHandler().unpackerHandler(client, jobId)
  println("Unpackerが呼び出されました")
  while( unpacker.hasNext() ) {
    val array = unpacker.unpackValue().asArrayValue()
    f1(array)
  }
  close()
  println("Finished access to TresureData Database.")
  System.exit(0)
}
