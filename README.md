# Kotlin TreasureData Driver

## 開発の経緯

### 諸問題
- 非構造化データを分析できるエンジニア/サイエンティストが少なすぎる
- TresureDataという非構造化データをバックエンドでもち、表面的に構造化データとして扱えるサービスがあるが、このUIに特化してしまい、非構造化データと組み合わせて分析できる人がすくない。
- Apache Sparkと連携するたびに、TreasureDataのデータをWeb UIよりダンプして、Sparkに格納して、デシリアライズするという非現実的な複雑なオペレーションの解消

### 解決のアプローチ
- Scala Likeで使えるKotlinでTreasureDataをDriveして、非構造化データと、構造化データの両方を分析できるテンプレートを提供する

### 使い方
1. TreasureDataに投げるクエリをkotlinDriver.sqlに記述する
2. TreasureData.ktを編集して、どのような出力を得たいか、プログラミングする
3. compile.treasureData.shを実行してコンパイル
4. run.treasureData.shを実行してtreasureDataのデータの取得して結果を得る

### セットアップ
プログラムをダウンロードする
```sh
$ git clone gink03/kotlin-treasuredata-driver
```
依存するjarをダウンロードする
```sh
$ sh download-setup-jars.sh
```

### 任意の編集する箇所
fraction関数の中を任意に書き換えればよい  
```kotlin
import com.treasuredata.client.*
import com.google.common.base.Function
import org.msgpack.core.MessagePack
import org.msgpack.core.MessageUnpacker
import org.msgpack.value.ArrayValue
import com.treasuredata.client.model.*
import java.io.InputStream
import java.io.File

fun fraction(array: ArrayValue ) {
  // ここを自由に編集して、任意の実装をしてください
  println(array)
}


fun main(args: Array<String>) {
  val client = TDClient.newClient();
  println("Start connecting to TreasureData Database.")
  val names:List<String> = client.listDatabases().map { db ->
    db.getName().toString()
  }.toList()
  names.map { name -> println("There is Database of ${name}.") }
  val jobId = client.submit(TDJobRequest.newPrestoQuery("dac_aonesync",
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
```
