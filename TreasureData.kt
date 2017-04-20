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

import com.google.gson.Gson                                                                                                                                                                                                    import com.google.gson.GsonBuilder

val gson = Gson()
fun fraction(array: ArrayValue ) { 
  println( gson.toJson(array.toList()) )
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
  var status = client.jobStatus(jobId).getStatus().isFinished()
  var counter:Int = 0
  while(!status ) {
    //Thread.sleep(backOff.nextWaitTimeMillis().toLong())
    /*try { */
      status  = client.jobStatus(jobId).getStatus().isFinished() 

      counter += 1
      println(counter)
    /*} catch (e : java.nio.channels.ClosedChannelException ) { 
      println(e)
    }*/
  }
  if ( !status ) { 
    println("Error can not be controled. will shutdown.")
    System.exit(0)
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
