package common;


import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.*;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2020/12/21 13:40
 */
public class ESClientFactory {

    public static void main(String[] args) throws IOException {


        RestHighLevelClient client = new RestHighLevelClient(
                //因为使用的是http, 所以这里是9200 ,不是对应的9300,这是个大坑!
                RestClient.builder(new HttpHost("localhost", 9200, "http")));



        client.close();

    }



    //------------------------------------------------------------------------------

    //创建客户端
    private RestHighLevelClient client;

    /**
     * 创建索引
     * @param index
     * @throws IOException
     */
    public void createIndex(String index) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(index);
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println("createIndex: " + JSON.toJSONString(createIndexResponse));
    }


        /**
         * 判断索引是否存在
         * @param index
         * @return
         * @throws IOException
         */
        public boolean existsIndex(String index) throws IOException {
            GetIndexRequest request = new GetIndexRequest();
            request.indices();
            boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
            System.out.println("existsIndex: " + exists);
            return exists;

        }



    /**
     * 增加记录
     * @param index
     * @param type
     * @param tests
     * @throws IOException
     */
    public void add(String index, String type, Tests tests) throws IOException {
        IndexRequest indexRequest = new IndexRequest(index, type, tests.getId().toString());
        indexRequest.source(JSON.toJSONString(tests), XContentType.JSON);
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("add: " + JSON.toJSONString(indexResponse));
    }


    /**
     * 获取记录信息
     * @param index
     * @param type
     * @param id
     * @throws IOException
     */
    public void get(String index, String type, Long id) throws IOException {
        GetRequest getRequest = new GetRequest(index, type, id.toString());
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println("get: " + JSON.toJSONString(getResponse));
    }

    /**
     * 更新记录信息
     * @param index
     * @param type
     * @param tests
     * @throws IOException
     */
    public void update(String index, String type, Tests tests) throws IOException {
        tests.setName(tests.getName() + "updated");
        UpdateRequest request = new UpdateRequest(index, type, tests.getId().toString());
        request.doc(JSON.toJSONString(tests), XContentType.JSON);
        UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
        System.out.println("update: " + JSON.toJSONString(updateResponse));
    }

    /**
     * 删除记录
     * @param index
     * @param type
     * @param id
     * @throws IOException
     */
    public void delete(String index, String type, Long id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(index, type, id.toString());
        DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println("delete: " + JSON.toJSONString(response));
    }


    /**
     * 搜索
     * @param index
     * @param type
     * @param name
     * @throws IOException
     */
    public void search(String index, String type, String name) throws IOException {
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        boolBuilder.must(QueryBuilders.matchQuery("name", name)); // 这里可以根据字段进行搜索，must表示符合条件的，相反的mustnot表示不符合条件的
        // boolBuilder.must(QueryBuilders.matchQuery("id", tests.getId().toString()));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolBuilder);
        sourceBuilder.from(0);
        sourceBuilder.size(100); // 获取记录数，默认10
        sourceBuilder.fetchSource(new String[]{"id", "name"}, new String[]{}); // 第一个是获取字段，第二个是过滤的字段，默认获取全部
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);
        searchRequest.source(sourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("search: " + JSON.toJSONString(response));
        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            System.out.println("search -> " + hit.getSourceAsString());
        }
    }



    //变量
    public static String INDEX_TEST = null;
    public static String TYPE_TEST = null;
    public static List<Tests> testsList = null;
    public static Tests tests = null;

        /**
         * 批量操作
         * @throws IOException
         */
        public void bulk() throws IOException {
            // 批量增加
            BulkRequest bulkAddRequest = new BulkRequest();

            for (int i = 0; i < testsList.size(); i++) {
                tests = testsList.get(i); //获取每一行数据
                //index,type,id
                IndexRequest indexRequest = new IndexRequest(INDEX_TEST, TYPE_TEST, tests.getId().toString());
                indexRequest.source(JSON.toJSONString(tests), XContentType.JSON);
                bulkAddRequest.add(indexRequest);
            }
            BulkResponse bulkAddResponse = client.bulk(bulkAddRequest, RequestOptions.DEFAULT);
            search(INDEX_TEST, TYPE_TEST, "this");



            // 批量更新
            BulkRequest bulkUpdateRequest = new BulkRequest();
            for (int i = 0; i < testsList.size(); i++) {
                tests = testsList.get(i);
                tests.setName(tests.getName() + " updated");
                //根据指定的id批量更新
                UpdateRequest updateRequest = new UpdateRequest(INDEX_TEST, TYPE_TEST, tests.getId().toString());
                updateRequest.doc(JSON.toJSONString(tests), XContentType.JSON);
                bulkUpdateRequest.add(updateRequest);
            }
            BulkResponse bulkUpdateResponse = client.bulk(bulkUpdateRequest, RequestOptions.DEFAULT);
            System.out.println("bulkUpdate: " + JSON.toJSONString(bulkUpdateResponse));
            search(INDEX_TEST, TYPE_TEST, "updated");




            // 批量删除
            BulkRequest bulkDeleteRequest = new BulkRequest();
            for (int i = 0; i < testsList.size(); i++) {
                tests = testsList.get(i);
                DeleteRequest deleteRequest = new DeleteRequest(INDEX_TEST, TYPE_TEST, tests.getId().toString());
                bulkDeleteRequest.add(deleteRequest);
            }
            BulkResponse bulkDeleteResponse = client.bulk(bulkDeleteRequest, RequestOptions.DEFAULT);
            System.out.println("bulkDelete: " + JSON.toJSONString(bulkDeleteResponse));
            search(INDEX_TEST, TYPE_TEST, "this");
        }


            //--------------------------------------------------------------


            /**
             * 更新doc
             * @param client
             * @throws IOException
             */
    private static void updateDoc(RestHighLevelClient client) throws IOException {
        //不存在的更新会报错
        Map<String, Object> jsonMap = new HashMap<String, Object>();
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "daily update");
        //指定id
        UpdateRequest updateRequest = new UpdateRequest("posts", "3").doc(jsonMap);
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        //插入数据
        System.out.println(updateResponse.status());
    }




    /**
     * 删除文档
     * @param client
     * @throws IOException
     */
    private static void deleteDoc(RestHighLevelClient client) throws IOException {
        DeleteRequest request = new DeleteRequest("posts", "1");
        DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.status() + "~" + delete.getResult());
    }


    /**
     * 文档是否存在
     * @param client
     * @throws IOException
     */
    private static void existsDoc(RestHighLevelClient client) throws IOException {
        for (int i = 1; i <5; i++) {
            GetRequest request = new GetRequest("posts", String.valueOf(i));
            request.fetchSourceContext(new FetchSourceContext(false));
            request.storedFields("_none_");
            boolean exists = client.exists(request, RequestOptions.DEFAULT);
            System.out.println(exists);
        }


        /*GetRequest request2 = new GetRequest("posts", "5");
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");
        boolean exists2 = client.exists(request2, RequestOptions.DEFAULT);
        System.out.println(exists2);*/

    }



    /**
     * 获取一个文档
     * @param client
     * @throws IOException
     */
    private static void getDoc(RestHighLevelClient client) throws IOException {
        //没有posts 会报错的
        GetRequest posts = new GetRequest("posts", "1");
        GetResponse response = client.get(posts, RequestOptions.DEFAULT);
        System.out.println(response.getId());
        System.out.println(response.getIndex());
        System.out.println(response.getSourceAsString());
        System.out.println(response.getSourceAsMap());
    }




    /**
     * 创建一个文档
     * @param client
     * @throws IOException
     */
    private static void createDoc(RestHighLevelClient client) throws IOException {
        IndexRequest request = new IndexRequest("posts");
        request.id("1");
        String jsonString = "{" +
                "\"user\":\"zk\"," +
                "\"postDate\":\"2019-01-30\"," +
                "\"message\":\"trying out java\"" +
                "}";
        String jsonString2 = "{" +
                "\"user\":\"zk2\"," +
                "\"postDate\":\"2019-10-01\"," +
                "\"message\":\"trying out javascript\"" +
                "}";
        //为什么这里要有个jsonString2 ,因为这个request.source(jsonString, jsonString2);
        // 的参数数量必须是偶数, 不然会报错的.
        request.source(jsonString, jsonString2);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println(indexResponse.getResult());


        Map<String, Object> jsonMap = new HashMap<String, Object>();
        jsonMap.put("user", "ting");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out hadoop");
        IndexRequest post = new IndexRequest("post").id("2").source(jsonMap);
        client.index(post, RequestOptions.DEFAULT);


        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("user", "wang");
            builder.timeField("postDate", new Date());
            builder.field("message", "trying out scala");
        }
        builder.endObject();
        IndexRequest posts = new IndexRequest("posts").id("3").source(builder);
        client.index(posts, RequestOptions.DEFAULT);


        IndexRequest source = new IndexRequest("posts").id("4").source("user", "peng",
                "postDate", new Date(),
                "message", "trying out golang");

        client.index(source, RequestOptions.DEFAULT);
    }

}

















//    private static final String HOST = "127.0.0.1";
//    private static final int PORT = 9200;
//    private static final String SCHEMA = "http";
//    private static final int CONNECT_TIME_OUT = 1000;
//    private static final int SOCKET_TIME_OUT = 30000;
//    private static final int CONNECTION_REQUEST_TIME_OUT = 500;
//
//    private static final int MAX_CONNECT_NUM = 100;
//    private static final int MAX_CONNECT_PER_ROUTE = 100;
//
//    private static HttpHost HTTP_HOST = new HttpHost(HOST,PORT,SCHEMA);
//    private static boolean uniqueConnectTimeConfig = false;
//    private static boolean uniqueConnectNumConfig = true;
//    private static RestClientBuilder builder;
//    private static RestClient restClient;
//    private static RestHighLevelClient restHighLevelClient;
//
//    static {
//        init();
//    }
//
//    public static void init(){
//        builder = RestClient.builder(HTTP_HOST);
//        if(uniqueConnectTimeConfig){
//            setConnectTimeOutConfig();
//        }
//        if(uniqueConnectNumConfig){
//            setMutiConnectConfig();
//        }
//        restClient = builder.build();
//        restHighLevelClient = new RestHighLevelClient(restClient);
//    }
//
//
//
//
//    // 主要关于异步httpclient的连接延时配置
//    public static void setConnectTimeOutConfig(){
//        builder.setRequestConfigCallback(new RequestConfigCallback() {
//
//            @Override
//            public Builder customizeRequestConfig(Builder requestConfigBuilder) {
//                requestConfigBuilder.setConnectTimeout(CONNECT_TIME_OUT);
//                requestConfigBuilder.setSocketTimeout(SOCKET_TIME_OUT);
//                requestConfigBuilder.setConnectionRequestTimeout(CONNECTION_REQUEST_TIME_OUT);
//                return requestConfigBuilder;
//            }
//        });
//    }
//    // 主要关于异步httpclient的连接数配置
//    public static void setMutiConnectConfig(){
//        builder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
//
//            @Override
//            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//                httpClientBuilder.setMaxConnTotal(MAX_CONNECT_NUM);
//                httpClientBuilder.setMaxConnPerRoute(MAX_CONNECT_PER_ROUTE);
//                return httpClientBuilder;
//            }
//        });
//    }
//
//    public static RestClient getClient(){
//        return restClient;
//    }
//
//    public static RestHighLevelClient getHighLevelClient(){
//        return restHighLevelClient;
//    }
//
//    public static void close() {
//        if (restClient != null) {
//            try {
//                restClient.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }





