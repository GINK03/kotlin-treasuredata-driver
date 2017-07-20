# Kotlin TreasureData Driver

<p align="center">
  <img src="https://cloud.githubusercontent.com/assets/4949982/24949397/6cb8e15c-1fa8-11e7-94d6-e96622d1f4b4.png">
</p>


## 開発の経緯

### 諸問題
- TresureDataという非構造化データをバックエンドでもち、表面的に構造化データとして扱えるサービスがあるが、このUIに特化してしまい、非構造化データと組み合わせて分析できる人がすくない。
- Apache Sparkと連携するたびに、TreasureDataのデータをWeb UIよりダンプして、Sparkに格納して、デシリアライズするという非現実的な複雑なオペレーションの解消
- 定期タスク化の促進（一度jarファイルで出力してしまえば、cronに登録しておけば、自動で実行可能）
- 技術レベルの向上(Java以外の関数型言語等は使えた方がいい)

### 解決のアプローチ
- Scala Likeで使えるKotlinでTreasureDataを構造化して、非構造化データと、構造化データの両方を分析できるテンプレートを提供する

### td-command toolbeltのセットアップ
```console
$ curl -L https://toolbelt.treasuredata.com/sh/install-ubuntu-trusty-td-agent2.sh | sh
```

```console
$ td -e https://api.treasuredata.com account -f
Email: user@domain.com
Password (typing will be hidden):
Authenticated successfully.
```


### 使い方
1. TreasureDataに投げるクエリをkotlinDriver.sqlに記述する
2. TreasureData.ktを編集して、どのような出力を得たいか、プログラミングする
3. compile.treasureData.shを実行してコンパイル
4. run.treasureData.shを実行してtreasureDataのデータの取得して結果を得る

### セットアップ & 実行
1. プログラムをダウンロードする
```console
$ git clone gink03/kotlin-treasuredata-driver
```
2. 依存するjarをダウンロードする
```sh
$ sh download-setup-jars.sh
```
3. 基礎プログラムをコンパイルする
```console
$ sh compile.tdhandler.sh
```
4. (編集し終わったら)全体をコンパイルする
```console
$ sh compile.treasureData.sh 
```

### 実行
td-clientのセットアップを行なったあとに行なってください
```console
$ sh run.treasureData.sh
```
(この状態で実行すると、直近のログを全てダウンロードします)

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

## ToDo
- mavenかgradleで管理したい
