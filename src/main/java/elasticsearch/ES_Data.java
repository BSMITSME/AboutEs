package elasticsearch;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Base64;

public class ES_Data {
    private static RestClient client;
    private static Request request;

    public void getClient() {
        String CREDENTIALS_STRING = "id:pw";
        String encodedBytes = Base64.getEncoder().encodeToString(CREDENTIALS_STRING.getBytes());
        Header[] auto_headers_testCluster = {
                new BasicHeader("Authorization", "Basic " + encodedBytes)
        };
        client = RestClient.builder(
                new HttpHost("localhost", 9200, "http")
//                new HttpHost("192.xxx.xx.xx", 9200, "http"),
//                new HttpHost("192.xxx.xx.xx", 9200, "http"),
//                new HttpHost("192.xxx.xx.xx", 9200, "http")
        ).setDefaultHeaders(auto_headers_testCluster).build();
    }
    public JSONArray getData(String index, int size, String search_after_value) throws IOException {
        Response rs = null;
        request = new Request("GET", index+"/_search");
        request.setEntity(new NStringEntity(query(size,search_after_value),ContentType.APPLICATION_JSON));

        rs = client.performRequest(request);
        String rs_str = EntityUtils.toString(rs.getEntity());
        JSONObject jo = new JSONObject(rs_str);
//        System.out.println(jo);
        JSONArray jsonArray = jo.getJSONObject("hits").getJSONArray("hits");

        return jsonArray;
    }
    public String query(int size, String search_after_value){
        String query = "{\n" +
                "  \"size\" : " + size + ",\n" +
                "  \"query\" : {\n" +
                "    \"query_string\" : {\n" +
                "      \"query\" : \"*\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"_source\" : [\"kw_docid\",\"wc_sitename\", \"in_date\", \"au_domain\", \"an_title\", \"au_url\"],\n" +
                "  \"sort\": [\n" +
                "    {\n" +
                "      \"kw_docid.keyword\": {\n" +
                "        \"order\": \"asc\"\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  \"search_after\": [\"" + search_after_value + "\"]\n" +
                "}";
        return query;
    }
    public void closeConnecction(){
        try{
            client.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
