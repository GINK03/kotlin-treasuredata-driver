import java.io.*
import java.net.URLDecoder

import org.iq80.leveldb.*
import org.fusesource.leveldbjni.JniDBFactory.*
import org.iq80.leveldb.Options

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.google.gson.GsonBuilder

import java.util.Random

val header = File("aone.tables.csv")
  .readText()
  .split("\n").filter { x -> 
    x != "" 
  }.map { x ->
    x.split("\t").last()
  }.toList()

val options = org.iq80.leveldb.Options()
var db:DB?  = null

fun _getDb():DB { 
  options.createIfMissing(true)
  if( db == null ) {
    db = factory.open(File("tuuidInv"), options)
  }
  return db!!
}

fun _closeDb() {
  if( db != null ) {
    db!!.close()
    db = null
  }
}

val type = object : TypeToken<MutableList<Map<String, Any>>>() {}.type
var counter:Int = 0

fun tuuidInv(array: List<String>) { 
  counter += 1
  val db = _getDb()
  val batch:WriteBatch = db.createWriteBatch()
  val zip:MutableMap<String,Any> = header.zip(array)
    .map { x ->
      val (k,v) = x
      Pair<String, Any>(k, v)
    }.toMap().toMutableMap()
  if( zip["tuuid"] == null || zip["tuuid"] == "null" || zip["tuuid"] == "opt-out" ) return 
  try {
    val request_uri = URLDecoder.decode(URLDecoder.decode(zip.get("request_uri").toString()))
      .split("&").map { x -> 
        x.split("=")
      }.filter { xs ->
        xs.size == 2
      }.map { xs ->
        val (k, v) = xs
        Pair(k, v)
      }.toMap()
    zip["request_uri"] = request_uri
    //println(zip)
    val tuuid = zip["tuuid"]!!.toString()
    
    val json = asString(db.get(bytes(tuuid)))
    if( json == null ) {
      batch.put(bytes(tuuid), bytes(gson.toJson(listOf(zip))))
      if( Random().nextInt(1000) <= 1 ) 
        printerr("${counter} init ${tuuid}...ramdoness ${Random().nextInt(1000)}")
    } else { 
      val cnt:MutableList<Map<String,Any>> = gson.fromJson<MutableList<Map<String, Any>>>(json, type)
      cnt.add(zip)
      batch.put(bytes(tuuid), bytes(gson.toJson(cnt)))
      if( Random().nextInt(1000) <= 1 ) 
        printerr("${counter} update ${tuuid}...ramdoness ${Random().nextInt(1000)}")
    }
  }catch(e: java.lang.IllegalArgumentException) {
  }
  db.write(batch) 
  batch.close()
}
