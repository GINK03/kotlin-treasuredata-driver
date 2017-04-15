
import java.io.File 
import java.io.FileReader
import java.io.BufferedReader
import java.net.URLDecoder
import java.io.UnsupportedEncodingException
import java.lang.Exception

fun <T> printerr(t : T) { 
  System.`err`.println(t)
}


fun epochConv(readable: String): Int? { 
  val epoch_base = 719161;
  try {
    val (head, tail) = readable.split(" ")
    val (year, month, day) = head.split("-").map { x -> x.toInt() }
    val (hour, minute, second) = tail.split(":").map { x -> x.toInt() } 
    val epoch = ((365 * year + year/4 
            - year/100 
            + year/400 
            + 306*(month + 1)/10 
            - 428+day) - epoch_base)*86400 
            + hour*3600 + minute*60 + second
    return epoch
  } catch (e: java.lang.IndexOutOfBoundsException) {
    return null
  }
}

fun minify() { 
  val br = BufferedReader(FileReader("../datasets/20160414.tsv"))
  val head = br.readLine().split("\t")
  var counter:Int = 0
  while(true) {
    counter += 1
    if( counter % 1000 == 0 ) { 
      printerr("now iter ${counter}")
    }
    val line = br.readLine()
    if( line == null) break
    val ents = line.split("\t")
    val zip  = head.zip(ents).toMap()
    // 特定のドメインのみフィルタリング
    if( zip["referer"] == null || !zip["referer"]!!.contains("suumo") ) continue 
    if( zip["keywords"] == null ) continue

    val time = ents[0]
    if( time == null ) continue 
    val decoded = ents.getOrElse(2) { null  }
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
    val keywords = zip["keywords"]
    val nowWatch = zip["referer"]
    if( keywords != "null" ) { 
      println("${keywords} ${time} ${nowWatch}")
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
  val MODE:String? = args.getOrElse(0) { null }
  when(MODE) { 
    null -> { println("no mode specified"); System.exit(0) }
    "minify" -> { minify() }
    "hatena" -> { checkHatena() }
  }
}
