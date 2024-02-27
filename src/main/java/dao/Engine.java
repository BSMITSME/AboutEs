package dao;

import dto.Lucy;
import elasticsearch.ES_Data;
import mybatis.SQLFactory;
import org.apache.ibatis.session.SqlSession;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Engine {
    ES_Data esData = null;
    JSONObject jsonObject_title = null;
    private SQLFactory sqlFactory;
    public SqlSession session;

    public void Engine_DAO(){
        sqlFactory = new SQLFactory();
        try{
            session = sqlFactory.GetSqlSession("con"); // myBatisConfig와 이름 맞추기?
//            insertData(session, 200);
            bulkInsertData(session,234);
            getCount(session);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public void getCount(SqlSession session){
        int num = session.selectOne("test_count");
        System.out.println(num);
    }
    public void bulkInsertData(SqlSession sqlSession, int size){
        ArrayList<Lucy> lucyArrayList = null;

        String search_after_value = "";

        int result = 0;

        Lucy lucy = null;

        esData = new ES_Data();

        try{
            esData.getClient();
            while(true){
                lucyArrayList = new ArrayList<>();
                System.out.println(search_after_value);

                // 쿼리를 만드는 부분.
                JSONArray arr = esData.getData("sample_lucy3_main_20240128_2000", size, search_after_value);

                // 데이터 가져오는 부분.
                // 가져온 데이터로 인서트 데이터를 만드는 부분.
                //  makeInsertData -> return
                //   --->
                System.out.println(arr.length());
                int arrlen = arr.length();
                if(arrlen != 0){
                    for (int i = 0; i<arrlen; i++){
                        lucy = new Lucy();

                        // lucy 객체 셋팅
                        lucy = makeLucy(lucy, arr, i);

                        // 셋팅된 Lucy 배열에 넣기
                        lucyArrayList.add(lucy);
                    }

                    // insert
                    result = sqlSession.insert("foreach_insert", lucyArrayList);
                    // paging
                    search_after_value = arr.getJSONObject(arr.length()-1).getJSONArray("sort").getString(0);



                    if(result < 1){
                        System.out.println("fail to insert");
                    }else {
                        System.out.println("Success");
                    }
                }else {
                    break;
                }
            }

        }catch (IOException e){
            e.printStackTrace();
        }catch (JSONException e) {
            e.printStackTrace();

        }finally {
            esData.closeConnecction();
        }
    }
    public Lucy makeLucy(Lucy lucy, JSONArray arr, int i){
        JSONObject cmd_shortcut = arr.getJSONObject(i).getJSONObject("_source");
        String sitename = cmd_shortcut.getString("wc_sitename");
        String domain = cmd_shortcut.getString("au_domain");
        String docid = cmd_shortcut.getString("kw_docid");
        int date = cmd_shortcut.getInt("in_date");
        String url = cmd_shortcut.getString("au_url");
        String title = cmd_shortcut.getString("an_title");

        lucy.set_an_title(title);
        lucy.set_au_domain(domain);
        lucy.set_au_url(url);
        lucy.set_in_date(date);
        lucy.set_kw_docid(docid);
        lucy.set_wc_sitename(sitename);

        return lucy;
    }

    // 알아서 해주긴 하지만 통신을 여러번 해야하므로 좋지 않은 선택 -> bulk로 insert 요청하는 방법으로 개선
    public void insertData(SqlSession sqlSession, int size){
        String search_after_value = "";
        int result = 0;
        Lucy lucy = null;
        esData = new ES_Data();

        try{
            esData.getClient();
            while(true){
                JSONArray arr = esData.getData("sample_lucy3_main_20240128_2000", size, search_after_value);
                search_after_value = arr.getJSONObject(arr.length()-1).getJSONArray("sort").getString(0);

                if(arr.length() == 0){
                    break;
                }

                for (int i = 0; i<size; i++){
                    lucy = new Lucy();
                    JSONObject cmd_shortcut = arr.getJSONObject(i).getJSONObject("_source");
                    String sitename = cmd_shortcut.getString("wc_sitename");
                    String domain = cmd_shortcut.getString("au_domain");
                    String docid = cmd_shortcut.getString("kw_docid");
                    int date = cmd_shortcut.getInt("in_date");
                    String url = cmd_shortcut.getString("au_url");
                    String title = cmd_shortcut.getString("an_title");

                    lucy.set_an_title(title);
                    lucy.set_au_domain(domain);
                    lucy.set_au_url(url);
                    lucy.set_in_date(date);
                    lucy.set_kw_docid(docid);
                    lucy.set_wc_sitename(sitename);

                    result = sqlSession.insert("insert_data", lucy);
                    if(result < 1){
                        System.out.println("fail to insert");
                    }else {
                        System.out.println("Success");
                    }
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            esData.closeConnecction();
        }
    }
}
