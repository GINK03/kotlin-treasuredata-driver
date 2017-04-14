
import java.io.File 
import java.io.FileReader
import java.io.BufferedReader
import java.net.URLDecoder
fun main(args: Array<String>) { 
  val br = BufferedReader(FileReader("../datasets/20160414.tsv"))
  val head = br.readLine().split("\t")
  println(head)
  while(true) {
    val line = br.readLine()
    if(line == null) break
    val ents = line.split("\t")
    val zip  = head.zip(ents).toMap()
    val time = ents[0]
    val raw  = URLDecoder.decode(ents[2]).split("&").map { x ->
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
