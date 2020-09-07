package com.atguigu.gmall.util

import java.util
import java.util.Properties

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import org.apache.lucene.queryparser.flexible.standard.builders.BooleanQueryNodeBuilder
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, RangeQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder

object MyEsUtil {
  def main(args: Array[String]): Unit = {
    //saveDoc(Movie("1", "乘风破浪"), "movie_test829")
    //测试查询
    search("movie_chn")

  }

  case class Movie(id: String, name: String)

  def search(indexname: String) = {
    val client: JestClient = getJestClint()
    val query = "{\n  \"aggs\": {\n    \"groupby\": {\n      \"terms\": {\n        \"field\": \"actorList.name\",\n        \"size\": 1000,\n        \"order\": {\n          \"groupavg\": \"asc\"\n        }\n      },\n        \"aggs\": {\n    \"groupavg\": {\n      \"avg\": {\n        \"field\": \"doubanScore\"\n      }\n    }\n        }\n    }\n  }\n}"


    //es提供了查询条件的封装工具
    val sourceBuilder = new SearchSourceBuilder
    val bool = new BoolQueryBuilder
    bool.filter(new RangeQueryBuilder("doubanScore").gte(8).lte(10))
    bool.must(new MatchQueryBuilder("name", "红海"))
    val builder: SearchSourceBuilder = sourceBuilder.query(bool)

    // println(builder.toString)


    val search: Search = new Search.Builder(builder.toString).addIndex(indexname).addIndex(indexname).build()
    val result: SearchResult = client.execute(search)
    val hits: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
    import collection.JavaConverters._
    for (hit <- hits.asScala) {
      val source: util.Map[String, Any] = hit.source
      println(source.get("name"))

    }
    client.close()

  }


  var jestClientFactory: JestClientFactory = _

  def getJestClint() = {
    if (jestClientFactory != null) {
      jestClientFactory.getObject
    } else {
      val properties: Properties = PropertiesUtil.load("config.properties")
      val host: String = properties.getProperty("elasticsearch.host")
      val port: String = properties.getProperty("elasticsearch.port")

      jestClientFactory = new JestClientFactory
      jestClientFactory.setHttpClientConfig(new HttpClientConfig.Builder("http://" + host + ":" + port).maxTotalConnection(4).multiThreaded(true).build())
      jestClientFactory.getObject
    }

  }

  //一条一条的传过来进行保存
  def saveDoc(doc: Any, indexName: String) = {
    val jestClint: JestClient = getJestClint
    //build中可以放样例类
    val index: Index = new Index.Builder(doc).index(indexName).`type`("_doc").build()
    jestClint.execute(index)
    jestClint.close()
  }

  //一批一批的传过来进行保存
  def saveDocBulk(docList: List[(Any, String)], indexName: String) = {
    val jestClint: JestClient = getJestClint
    val builder: Bulk.Builder = new Bulk.Builder
    builder.defaultIndex(indexName).defaultType("_doc")
    for ((doc, id) <- docList) {

      val index: Index = new Index.Builder(doc).id(id).build()
      builder.addAction(index)

    }
    val items: util.List[BulkResult#BulkResultItem] = jestClint.execute(builder.build()).getItems
    println("共提交" + items.size() +  "条数")
    jestClint.close()
  }


}




























