package cn.wscode.controller;

import cn.wscode.msg.Message;
import com.alibaba.fastjson.JSON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Date;

@SpringBootApplication
@Controller
@RequestMapping("ReportApplication")
public class ReportController {
    @Autowired
    private KafkaTemplate kafkaTemplate;

    /**
     * 接受数据
     * @param json
     * @param response
     */
    @RequestMapping("retrieveData")
    public void retrieveData(@RequestBody String json, HttpServletRequest request, HttpServletResponse response){
     //将获取的消息封装
        Message message = new Message();
        message.setCount(1);
        message.setTimestemp(new Date().getTime());
        message.setMessage(json);
        json = JSON.toJSONString(message);
        System.out.println(json);
        //发送消息体
       kafkaTemplate.send("topic","key",json);

        //结果回显
        PrintWriter printWriter = getWrite(response);
        response.setStatus(HttpStatus.OK.value());
        printWriter.write("success");
        close(printWriter);
    }

    private PrintWriter getWrite(HttpServletResponse response){
        response.setCharacterEncoding("utf-8");
        response.setContentType("application/json");
        //回显结果：IO流的操作
        OutputStream out = null;
        PrintWriter printWriter = null ;
        try{
            out = response.getOutputStream() ;
            printWriter = new PrintWriter(out);
        }catch(IOException e){
            e.printStackTrace();
        }
        return printWriter;
    }

    //关闭IO流
    public void close(PrintWriter printWriter){
        printWriter.flush();
        printWriter.close();
    }

}
