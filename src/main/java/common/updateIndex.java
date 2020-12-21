package common;


import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2020/12/21 10:04
 */
public class updateIndex {
//    public void updateIndex(TransportClient client){
//
//    }

    /**
     * 简单查询es 指定index type id
     */
    @Test
    public void search() throws UnknownHostException {
        //获取client
        Settings settings= Settings.builder().put("cluster.name","my-application").build();
        TransportClient transportClient=new PreBuiltTransportClient(settings);
        transportClient.addTransportAddress(new
                TransportAddress(InetAddress.getByName("localhost"),9300));
        // 发起请求得到响应
        GetResponse documentFields = transportClient.prepareGet("index3", "user3", "10").get();
        System.out.println(documentFields.getSource());
    }
    /**
     * 增加文档
     */
    @Test
    public void insert() throws Exception{
        Settings settings= Settings.builder().put("cluster.name","my-application").build();
        TransportClient client=new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));
        XContentBuilder contentBuilder= XContentFactory.jsonBuilder()
                .startObject()
                .field("catid","22")
                .field("classify",54)
                .field("author","ssve")
                .field("id","1")
                .field("title","菜鸟成长记")
                .endObject();
        IndexResponse indexResponse = client.prepareIndex("index3", "user3", "10")
                .setSource(contentBuilder)
                .get();
        System.out.println(indexResponse.status());
    }
    /**
     * 删除文档
     */
    @Test
    public void delete() throws Exception{
        Settings settings=Settings.builder().put("cluster.name","my-application").build();
        TransportClient client=new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));

        DeleteResponse deleteResponse = client.prepareDelete("index3", "user3", "10").get();

        System.out.println(deleteResponse.status());
    }
    /**
     * 修改文档
     */
    @Test
    public void update() throws Exception{
        Settings settings=Settings.builder().put("cluster.name","my-application").build();
        TransportClient client=new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));

        UpdateRequest request=new UpdateRequest();
        XContentBuilder contentBuilder=XContentFactory.jsonBuilder()
                .startObject()
                .field("id","555")
                .endObject();
        request.index("index3")
                .type("user3")
                .id("10")
                .doc(contentBuilder);
        UpdateResponse updateResponse = client.update(request).get();

        System.out.println(updateResponse.status());
    }
    /**
     *upsert使用 如有存在对应文档就修改  不存在就新增  需要指定修改的文档 和新增的文档
     */
    @Test
    public void upsert() throws IOException, ExecutionException, InterruptedException {
        Settings settings=Settings.builder().put("cluster.name","my-application").build();
        TransportClient client=new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));

        XContentBuilder builder=XContentFactory.jsonBuilder()
                .startObject()
                .field("catid","22")
                .field("classify",54)
                .field("author","zhang")
                .field("id","10")
                .field("title","菜鸟成长记")
                .endObject();
        UpdateRequest request=new UpdateRequest();
        IndexRequest indexRequest=new IndexRequest();
        indexRequest.index("index3")
                .type("user3")
                .id("11")
                .source(builder);
        request.index("index3")
                .type("user3")
                .id("11")
                .doc(new XContentFactory().jsonBuilder()
                        .startObject()
                        .field("id","i love you")
                        .endObject()
                ).upsert(indexRequest);
        UpdateResponse updateResponse = client.update(request).get();

        System.out.println(updateResponse.status());

    }
    /**
     *
     * 批量查询  multiGet
     */
    @Test
    public  void multiGet() throws Exception{
        Settings settings=Settings.builder().put("cluster.name","my-application").build();
        TransportClient client=new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));


        MultiGetRequest request=new MultiGetRequest();
        request.add("index3","user3","11");
        request.add("index3","user3","10");
        request.add("index3","user3","13");
        request.add("index3","user3","14");
        MultiGetResponse multiGetItemResponses = client.multiGet(request).get();

        for (MultiGetItemResponse response:multiGetItemResponses) {
            System.out.println(response.getResponse().getSourceAsString());
        }

    }
    /**
     *
     *  bulk实现批量增删改操作。
     */
    @Test
    public void bulk() throws Exception{
        Settings settings=Settings.builder().put("cluster.name","my-application").build();
        TransportClient client=new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        IndexRequest request=new IndexRequest();
        request.index("index3")
                .type("user3")
                .id("13")
                .source(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("catid","85")
                        .field("classify","85")
                        .field("author","宇宙无敌")
                        .field("id","漫威")
                        .field("title","漫威")
                        .endObject());

        IndexRequest request1=new IndexRequest();
        request1.index("index3")
                .type("user3")
                .id("14")
                .source(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("catid","55")
                        .field("classify","85")
                        .field("author","宇宙无敌")
                        .field("id","漫威")
                        .field("title","漫威")
                        .endObject());
        bulkRequestBuilder.add(request1);
        bulkRequestBuilder.add(request);
        BulkResponse bulkItemResponses = bulkRequestBuilder.get();
        System.out.println(bulkItemResponses.status());

    }
    /**
     *
     * 使用query查询  match_all 查询所有
     */
    @Test
    public void query() throws Exception{
        Settings settings=Settings.builder().put("cluster.name","my-application").build();
        TransportClient client=new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));

        MatchAllQueryBuilder builder= QueryBuilders.matchAllQuery();


        SearchRequestBuilder index3 = client.prepareSearch("index3")
                .setQuery(builder)
                .setSize(5);
        SearchResponse searchResponse = index3.get();
        SearchHits hits =  searchResponse.getHits();
        for (SearchHit hit:hits) {
            System.out.println(hit.getSourceAsString());
        }
    }
    /**
     *
     * 使用query查询  match 查询
     */
    @Test
    public void matchQuery() throws Exception{
        Settings settings=Settings.builder().put("cluster.name","my-application").build();
        TransportClient client=new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));

        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("title", "爱");
        SearchRequestBuilder index = client.prepareSearch("index3").setQuery(matchQueryBuilder).setSize(10);
        SearchHits hits = index.get().getHits();
        for (SearchHit hit:hits
        ) {
            System.out.println(hit.getSourceAsString());
        }
    }
    /**
     *
     * 使用query查询  mutilMatch 查询
     */
    @Test
    public void mutilMatchQuery() throws Exception{
        Settings settings=Settings.builder().put("cluster.name","my-application").build();
        TransportClient client=new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));

        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("周星驰影帝", "author","title");
        SearchRequestBuilder index = client.prepareSearch("index3").setQuery(multiMatchQueryBuilder).setSize(10);
        SearchHits hits = index.get().getHits();
        for (SearchHit hit:hits
        ) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     *
     * 使用query查询 termheterms查询
     */
    @Test
    public void termsMatchQuery() throws Exception{
        Settings settings=Settings.builder().put("cluster.name","my-application").build();
        TransportClient client=new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));

        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("title", "周星驰", "影帝");
        SearchRequestBuilder index = client.prepareSearch("index3").setQuery(termsQueryBuilder).setSize(10);
        SearchHits hits = index.get().getHits();
        for (SearchHit hit:hits
        ) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     *
     * 使用query查询  范围  通配符 前缀 模糊查询
     */
    @Test
    public void query1() throws Exception{
        Settings settings=Settings.builder().put("cluster.name","my-application").build();
        TransportClient client=new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));
        //范围查询
        //QueryBuilder queryBuilder = QueryBuilders.rangeQuery("classify").from("1").to("40");

        //通配符查询 * 零个或多个
//        QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("author","周*");

        //前缀查询
//        QueryBuilder queryBuilder = QueryBuilders.prefixQuery("author","周");

        //模糊查询 查询类型的
//        QueryBuilder queryBuilder = QueryBuilders.fuzzyQuery("id","aac");

        //id查询

        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("1","2","4");
        SearchRequestBuilder index = client.prepareSearch("index3").setQuery(queryBuilder).setSize(10);
        SearchHits hits = index.get().getHits();
        for (SearchHit hit:hits
        ) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     *
     * 聚合查询
     */
    @Test
    public void aggregation() throws Exception{
        Settings settings=Settings.builder().put("cluster.name","my-application").build();
        TransportClient client=new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));


        AggregationBuilder aggregationBuilder= AggregationBuilders.max("max").field("id");

        SearchResponse index3 = client.prepareSearch("index3").addAggregation(aggregationBuilder).get();

        Max max = index3.getAggregations().get("max");

        System.out.println(max.getValue());
    }
    /**
     *
     * queryString
     */
    @Test
    public void queryString() throws Exception{
        Settings settings=Settings.builder().put("cluster.name","my-application").build();
        TransportClient client=new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));
        // + 代表必须有 -代表没有
        QueryBuilder queryBuilder=QueryBuilders.queryStringQuery("+周星驰 -sss");
        SearchRequestBuilder index3 = client.prepareSearch("index3")
                .setQuery(queryBuilder)
                .setSize(10);
        SearchResponse searchResponse = index3.get();
        for (SearchHit hit:searchResponse.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     *
     * 组合查询
     */
    @Test
    public void boolQuery() throws Exception{
        Settings settings=Settings.builder().put("cluster.name","my-application").build();
        TransportClient client=new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));
        QueryBuilder queryBuilder=QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("author","周星驰"))
                .mustNot(QueryBuilders.matchQuery("title","梁朝伟"))
                .should(QueryBuilders.matchQuery("title","影帝"))
                .filter(QueryBuilders.rangeQuery("id").gte("1"));


        SearchRequestBuilder index3 = client.prepareSearch("index3")
                .setQuery(queryBuilder)
                .setSize(10);
        SearchResponse searchResponse = index3.get();
        for (SearchHit hit:searchResponse.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

}
