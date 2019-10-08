package org.cityplus.wys.httpconn;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * @Description
 * @Author franksen
 * @CREATE 2019-10-05-下午11:30
 */
public class HttpUtils {

    public static HttpURLConnection init(HttpURLConnection conn){
        conn.setDoInput(true);
        conn.setDoOutput(true);
        conn.setRequestProperty("charset","utf-8");
        conn.setRequestProperty("Content-Type","application/json");
        return conn;
    }

    /**
     * HttpGET请求
     */
    public static JSONObject getAccess(String urlStr) {

        HttpURLConnection conn = null;
        BufferedReader in = null;
        StringBuilder builder = null;
        JSONObject response = null;
        try {
            URL url = new URL(urlStr);
            conn = init((HttpURLConnection) url.openConnection());
            conn.setRequestMethod("GET");
            conn.connect();

            in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "utf-8"));
            String line = "";
            builder = new StringBuilder();
            while((line = in.readLine()) != null){
                builder.append(line);
            }

            response = JSON.parseObject(builder.toString());

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (conn!=null)
                conn.disconnect();
            try {
                if (in != null)
                    in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return response;
    }

    /**
     * HttpDelete请求
     */
    public static Boolean deleteAccess(String urlStr) {
        HttpURLConnection conn = null;
        try {
            URL url = new URL(urlStr);
            conn = init((HttpURLConnection) url.openConnection());
            conn.setRequestMethod("DELETE");
            conn.connect();

            conn.getInputStream().close();

            conn.disconnect();
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }

        return true;
    }



    /**
     * HttpPost请求
     */
    public static String postAccess(String urlStr, JSONObject data)  {
        HttpURLConnection conn = null;
        BufferedReader in = null;
        StringBuilder builder = null;
        DataOutputStream out = null;
        try {
            URL url = new URL(urlStr);
            conn = init((HttpURLConnection) url.openConnection());
            conn.setRequestMethod("POST");
            conn.connect();

            out = new DataOutputStream(conn.getOutputStream());
            out.write(data.toString().getBytes("utf8"));
            out.flush();

            in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "utf-8"));
            String line = "";
            builder = new StringBuilder();
            while((line = in.readLine()) != null){
                builder.append(line);
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (conn!= null)
                conn.disconnect();
            try {
                if (in!=null)
                    in.close();
                if (out!=null)
                    out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (builder != null)
            return builder.toString();
        return "";
    }
}
