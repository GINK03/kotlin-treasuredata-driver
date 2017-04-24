import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import java.io.*
import java.nio.file.*
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken

fun minifyDataset() { 
  val gson = Gson()
  val type = object : TypeToken<Map<String, String>>() {}.type
  BufferedReader(FileReader("google_adwords_daily.json"))
    .lines()
    .forEach { x ->
      try { 
        val map:Map<String,String> = gson.fromJson<Map<String, String>>(x, type)
        val minify = map.filter { kv ->
          val (k, v) = kv
          val notToWatch = setOf("Day", "Search Exact match IS", "Search Lost IS (rank)", "share", "Top of page CPC", "First page CPC", "Max. CPC", "Cost", "Clicks", "Conversions", "Destination URL", "Keyword state", "Search Impr. share")
          !notToWatch.contains(k)
        }.map { kv ->
          val (k, v) = kv
          //println(kv)
          Pair( k.replace(" ", "-"), v.replace(" ", "-") )
        }.map { kv ->
          val (k,v) = kv 
          "${k}___${v}"
        }.toList().joinToString(" , ")
        println(minify)
      } catch(e: com.google.gson.JsonSyntaxException) { 
      }
    }
}

fun indexFeat() {
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

fun weekdayLoader():Map<String, String> { 
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

fun executor() { 
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
        //println(zip?.get("Day"))
        //println(weekday) 
      }
  }  
}


fun main(args: Array<String>) { 
  println("this program dealiong weekday feat calculator...")
  if( args.toList().contains("--minify") ) {
    minifyDataset()
  }
  if( args.toList().contains("--exe") ) {
    executor()
  }
  if( args.toList().contains("--feat") ) {
    indexFeat()
  }
}

