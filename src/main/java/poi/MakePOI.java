package poi;

import elasticsearch.ES_Data;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.json.JSONArray;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MakePOI {
    public static String filepath = "";
    public static String fileNm = "";
    public static String[] columns = {"an_title", "au_domain", "au_url","in_date", "kw_docid", "wc_sitename"};
    public static String search_after_value = "";

    ES_Data esData = null;
    public void makeFile(int num, String index, int size) {
        esData = new ES_Data();
        SXSSFWorkbook workbook = null;
        SXSSFSheet sheet = null;

        try {
            int colsize = columns.length;
            switch (num){
                // 파일 하나에 다 넣는 방법
                case 1:
                    esData.getClient();

                    workbook = new SXSSFWorkbook();
                    sheet = workbook.createSheet();
                    makeColName(sheet);

                    int sav_count = 0;
                    while(true){
                        JSONArray hitshits = esData.getData(index, size, search_after_value);
                        int hitslen = hitshits.length();
                        int hitslow = 0;
                        if(hitslen != 0){
                            for(int i= sav_count; i< (sav_count+hitslen); i++){
                                Row row = sheet.createRow(i+1);
                                for(int  a = 0; a<colsize; a++){
                                    Cell cell = row.createCell(a);
                                    if(!columns[a].equals("in_date")){
                                        cell.setCellValue(hitshits.getJSONObject(i%size).getJSONObject("_source").getString(columns[a]));
                                    }else {
                                        cell.setCellValue(hitshits.getJSONObject(i%size).getJSONObject("_source").getInt(columns[a]));
                                    }
                                }
                                hitslow ++;
                            }
                            sav_count = sav_count+hitslen;
                            search_after_value = hitshits.getJSONObject((hitslen - 1)).getJSONArray("sort").getString(0);
                        }else{
                            break;
                        }
                    }
                    makeExcel(workbook);
                    break;

                    //size 당 파일 하나
                case 2 :
                    esData.getClient();
                    while(true){
                        JSONArray arr = esData.getData(index, size, search_after_value);
                        int hitslen = arr.length();
                        if(hitslen != 0){
                            sheet = workbook.createSheet();
                            for(int i = 0; i<size; i++){
                                Row row = sheet.createRow(i+1);
                                for(int a = 0; a<colsize; a++){
                                    Cell cell = row.createCell(a);
                                    if(!columns[a].equals("in_date")){
                                        cell.setCellValue(arr.getJSONObject(i%size).getJSONObject("_source").getString(columns[a]));
                                    }else {
                                        cell.setCellValue(arr.getJSONObject(i%size).getJSONObject("_source").getInt(columns[a]));
                                    }
                                }
                            }
                        }else {
                            break;
                        }
                    }
                    break;
            }
        }catch  (IOException e){
            e.printStackTrace();
        }finally {
            esData.closeConnecction();
        }
    }

    public void makeColName(SXSSFSheet sheet){
        Map<String, String> col_name = new HashMap<>();

        // 리스트를 매개변수로 바꿀 시 수정되어야 할 코드
        col_name.put("an_title", "제목");
        col_name.put("au_dmoain", "사이트");
        col_name.put("au_url", "사이트 주소");
        col_name.put("in_date", "날짜");
        col_name.put("kw_docid", "문서 번호");
        col_name.put("wc_sitename", "플랫폼");

        int colsize = columns.length;

        Row row_name = sheet.createRow(0);
        for(int i= 0; i< colsize; i++){
            Cell cell = row_name.createCell(i);
            cell.setCellValue(col_name.get(columns[i]));
        }
    }
    public void makeExcel(Workbook workbook) throws IOException{
        FileOutputStream out = new FileOutputStream(new File(filepath, fileNm));
        System.out.println("approach to workbook (POI)...");
        workbook.write(out);
        out.close();
    }
}
