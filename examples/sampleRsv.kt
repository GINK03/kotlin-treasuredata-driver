import java.io.File 
import java.io.FileReader
import java.io.BufferedReader
import java.net.URLDecoder
import java.io.UnsupportedEncodingException
import java.lang.Exception
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken

import java.text.SimpleDateFormat
import java.util.Random
fun <T> printerr(t : T) { 
  System.`err`.println(t)
}


fun epochConv(readable: String): Long? { 
  val df   = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val date = df.parse(readable)
  val epoch:Long = date.getTime()
  return epoch
}

fun minify() { 
  val br = BufferedReader(FileReader("./5bil.json"))
  val head = "date_time,ip,request_uri,ipao97_value,referer,useragent,tuuid,account_id,data_owner_id,os,os_version,browser,browser_version,url_category_ids,event_ids,keyword_group_ids,keywords,segment_ids,gender_age,income,marriage,occupation,frequency_of_ec_buying,amount_of_ec_buying,brand_vs_price_oriented,children_adult,children_university,children_high_school,children_middle_school,children_elementary_school,children_preschooler,work_location,home_location,work_location_zipcode,home_location_zipcode,carrier,time".split(",").toList<String>()
  var counter:Int = 0
  val gson = Gson()
  val listType = object : TypeToken<List<String>>() {}.type
  while(true) {
    if( counter % 1000 == 0 ) { 
      //printerr("now iter ${counter}")
    }
    val line = br.readLine()
    if( line == null) break

    val json:List<String>? = try { 
      gson.fromJson<List<String>>(line, listType) as List<String>
      } catch(e: com.google.gson.JsonSyntaxException ) { null 
      } catch(e: kotlin.TypeCastException ) { null 
      } catch(e: java.lang.NumberFormatException ) { null }
    if( json == null) continue
    val zip  = head.zip(json!!).toMap()
    // 特定のドメインのみフィルタリング
    if( !zip["referer"]!!.contains("suumo") ) continue 
    val time = json.getOrElse(0) { null }
    if( time == null ) continue 
    val decoded = json.getOrElse(2) { null  }
    if( decoded == null ) continue
    var urlDecoded:String? = try{URLDecoder.decode(decoded)} catch(e:Exception) {null}
    if( urlDecoded == null ) continue
    val raw  = urlDecoded!!.split("&").map { x ->
      x.split("=")
    }.filter{ xs ->
      xs.size == 2
    }.map { xs ->
      Pair(xs[0], xs[1])
    }.toMap()
    //println(raw)
    val realReferrer = raw["ref"] // こっちが真のリファラ
    val realSrc      = raw["src"] // こっちが真のソース
    val keywords = zip["keywords"]
    val nowWatch = zip["referer"] // スペルミス & 今見てるURLっぽい 
    val ipao9702 = try { URLDecoder.decode(raw["ipao9702"]) } catch(e: Exception) { null } 
    if( ipao9702 != null ) { 
      val flag = if( ipao9702.contains("?") ) { "ng" } else { "ok" } 
      val outPut = "${epochConv(time)} ${time.replace(" ", "_")} ${ipao9702} ${flag} encSrc=${realSrc} encRef=${realReferrer} origRef=${nowWatch}"
      if( !outPut.contains("search.yahoo.co.jp") ) continue
      counter += 1
      if( outPut.contains("ng") ) { 
        //printerr("err ${outPut}")
        //printerr("${urlDecoded}")
        println("${counter} ng")
      } else {
        //printerr("${urlDecoded}")
        println("${counter} ok")
      }
      // println(outPut)
      if( Random().nextInt(100) < 5 ) { 
        //printerr("${counter} ${epochConv(time)} ${time.replace(" ", "_")} ${ipao9702} ${flag} encSrc=${realSrc} encRef=${realReferrer} origRef${nowWatch}")
      }
    }
  }
}

fun checkHatena() { 
  val br = BufferedReader(FileReader("./sampleRsv.csv"))
  var counter:Int = 0
  while(true) {
    counter += 1
    if( counter % 1000 == 0 ) { 
      printerr("now iter ${counter}")
    }
    val line = br.readLine()
    try { 
      line.split(" ")
    } catch(e: Exception ) { 
      continue
    }
    if( line.split(" ")[0].contains("？") ) { 
      println(line)
    }
  }
}

fun main(args: Array<String>) { 
  val MODE:String? = args.last()
  println(args.toList())
  when(MODE) { 
    null -> { println("no mode specified"); System.exit(0) }
    "--minify" -> { minify() }
    "--hatena" -> { checkHatena() }
    "--jsonize" -> jsonize()
  }
}
