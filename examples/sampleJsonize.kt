import java.io.File 
import java.io.FileReader
import java.io.BufferedReader
import java.net.URLDecoder
import java.io.UnsupportedEncodingException
import java.lang.Exception

// 以下gson
import com.google.gson.Gson
import com.google.gson.GsonBuilder

//　以下gzip steam
import java.io.FileOutputStream
import java.io.IOException
import java.util.zip.GZIPOutputStream
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

import java.util.stream.Collectors
fun jsonize() { 
  val gson = Gson()
  val br = BufferedReader(FileReader("../datasets/20160414.tsv"))
  val head = br.readLine().split("\t")
  for( eachFile in (0..10) ) { 
    var each:Int = 1
    var counter:Int = 0
    val buff:MutableList<String> = mutableListOf()
    for( line in br.lines() ) { 
      counter += 1
      if( counter % 1000 == 0 ) { 
        printerr("now iter ${counter}")
      }
      if( counter > 100000 ) { 
        val gout = GZIPOutputStream(FileOutputStream("tokyoComplete${eachFile}.gz"))
        gout.write(buff.joinToString("\n").toByteArray(StandardCharsets.UTF_8))
        break
      }
      val ents = line.split("\t")
      val zip:MutableMap<String, Any> = mutableMapOf()
      head.zip(ents).map { kv ->
        val (k, v) = kv
        zip[k] = v
      }

      val time = ents[0]
      val decoded = ents.getOrElse(2) { null  }
      var urlDecoded:String? = try{URLDecoder.decode(decoded)} catch(e:Exception) {null}
      val raw  = when(urlDecoded) { 
        is String -> urlDecoded?.split("&").map { x ->
          x.split("=")
          }.filter{ xs ->
            xs.size == 2
          }.map { xs ->
            Pair(xs[0], xs[1])
          }.toMap() 
        else -> "None"
      }
      val keywords = zip["keywords"]
      val nowWatch = zip["referer"]
      zip["request_uri"] = raw
      val json = gson.toJson(zip)
      buff.add(json)
    }
  }
}
