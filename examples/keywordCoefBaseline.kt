import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import java.io.*
import java.nio.file.*
import java.net.URLDecoder
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import kotlin.concurrent.thread
import java.lang.Thread

data class Data(val c:MutableList<Pair<String, String>> = mutableListOf() )


fun coefDataset() { 
  val gson = Gson()
  val type = object : TypeToken<List<String>>() {}.type
  val head = File("./tableDefines.txt").readText().split("\n").map { x ->
    x.split("\t").first()
  }
  val br = BufferedReader(FileReader("5bil.json"))
  val tuuid_data:MutableMap<String, Data> = mutableMapOf()
  var count = 0
  while(true) {
    val line = br.readLine()
    if ( count > 100000000 ) break
    if ( count % 10000 == 0 ) { 
      printerr("now iter ${count} activeThreads=${Thread.activeCount()}")
    }
    count += 1
    if ( line == null) break
    thread { 
      try { 
        val map:List<String> = gson.fromJson<List<String>>(line!!, type)
        val zip:MutableMap<String,Any> = head.zip(map).map { kv ->
          val(k, v) = kv
          Pair(k, v)
        }.toMap().toMutableMap()
        if( zip.get("tuuid") == null || zip.get("tuuid") == "null" ) throw Exception()
        try { 
          val request_uri = URLDecoder.decode(zip["request_uri"].toString())
              .split("?").last()
              .split("&").filter { x -> x.contains("=") }
              .map { x ->
                val (k, v) = x.split("=")
                Pair(k,URLDecoder.decode(v))
          }.toMap()
          zip["request_uri"] = "${request_uri}"
          if( request_uri.get("ipao9702") == null ) throw Exception()
          val tuuid = zip["tuuid"].toString()
          val data  = Pair( zip["date_time"].toString(), request_uri!!["ipao9702"]!! )
          when(tuuid_data.get(tuuid)) {
            null  -> { 
              tuuid_data[tuuid] = Data()
              tuuid_data[tuuid]!!.c.add( data )
            }
            else -> {
              tuuid_data[tuuid]!!.c.add( data )
            }
          }
        } catch(e: java.lang.IllegalArgumentException) { 
          throw Exception()
        }
      } catch(e: com.google.gson.JsonSyntaxException) { 
      } catch(e: java.lang.IllegalStateException) {
      } catch(e: Exception) {
      }
    }
  }
  tuuid_data.map { kv -> 
    val (tuuid, value) = kv
    val json = gson.toJson(value)
    println("${tuuid} ${json}")
  }
}

fun _indexFeat() {
  val featIndex:MutableMap<String,Int> = mutableMapOf()
  BufferedReader(FileReader("minify.csv"))
    .lines()
    .forEach { x ->
      val ents = x.split(" , ").filter { x -> !x.contains("Impressions") }
      ents.map { x ->
        if(featIndex.get(x) == null ) 
          featIndex[x] = featIndex.size
      }
    }
  val result = featIndex.map { kv -> 
    val (k,v) = kv
    "${k} ${v}"
  }.joinToString("\n")
  PrintWriter("featIndex.csv").append(result).close()
}

fun _weekdayLoader():Map<String, String> { 
  return File("weekday.csv").readText().split("\n")
    .filter { x -> x != "" }
    .map { x ->
      val ents = x.split(" ")
      val day  = ents.slice( (0..2) ).map { x ->
        String.format("%02d", x.toInt() )
      }.joinToString("-")
      val meta = ents[3]
      Pair(day, meta.toString())
    }.toMap<String, String>()
}

fun _executor() { 
  val weekday = weekdayLoader()
  val gson = Gson()
  Files.newDirectoryStream(Paths.get("download"), "*.gz").forEach { name ->  
    println(name)
    var isHead = 0
    var header:List<String> = listOf()
    BufferedReader(InputStreamReader(GZIPInputStream(FileInputStream("${name}"))))
      .lines()
      .map { line ->
        val zip = when( isHead ) {
          0 -> null
          1 -> { 
            header = line.split(",")
            null 
          }
          else -> {
            val ents = line.split(",")
            val zip  = header.zip(ents).toMap()
            zip
          }
        }
        isHead += 1
        zip 
      }.filter { zip ->
        zip != null
      }.filter { zip ->
        var ret:Boolean 
        try { 
          ret = ( (zip?.get("Impressions") != "0" )
            && (zip?.get("Impressions")!!.toDouble() >= 5.0 ) )
        } catch(e:Exception) { 
          ret = false 
        }
        ret 
      }.forEach { zip ->
        val m = zip?.toMutableMap()
        m!!["Weekday"] = weekday[zip?.get("Day")] ?: "None"
        val json = gson.toJson(m)
        println(json)
      }
  }  
}


fun main(args: Array<String>) { 
  println("this program dealiong weekday feat calculator...")
  if( args.toList().contains("--coef") ) {
    coefDataset()
  }
  if( args.toList().contains("--exe") ) {
    executor()
  }
  if( args.toList().contains("--feat") ) {
    indexFeat()
  }
}

