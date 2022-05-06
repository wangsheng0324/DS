package cn.wscode;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * Created by angel
 */
public class KafkaProoducerTest {
    public static void main(String[] args) {
        String message = "测试用例";
        String address = "http://localhost:8888/ReportApplication/retrieveData";
        try{
            URL url = new URL(address);
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoInput(true);//true:可以通过IO流读取数据
            conn.setDoOutput(true);
            conn.setAllowUserInteraction(true);//允许用户进行上下文中对URL进行检查
            conn.setUseCaches(false);
            conn.setReadTimeout(6*1000);
            //浏览器类型
            conn.setRequestProperty("User-agent" , "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36");
            conn.setRequestProperty("Content-Type" , "application/json");
            conn.connect();
            //使用IO流将结果发送出去
            OutputStream outputStream = conn.getOutputStream();
            BufferedOutputStream out = new BufferedOutputStream(outputStream);
            out.write(message.getBytes());
            out.flush();
            //发送完毕之后，让客户端能够看到返回的状态
            String tmp = "";
            InputStream in = conn.getInputStream();
            byte[] bytes = new byte[1024];
            while (in.read(bytes , 0 , 1024) != -1){
                tmp += new String(bytes);
            }
            System.out.println(conn.getResponseCode());
            System.out.println(tmp);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
