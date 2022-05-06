package cn.wscode.msg;

import lombok.Data;

/**
 * @author wangSheng
 * @title: Message
 * @projectName DS
 * @description: TODO 消息
 * @date 2022/3/23 14:36
 */
@Data
public class Message {
    private  int count; //消息次数
    private Long timestemp; //时间
    private String message; //消息体
}
