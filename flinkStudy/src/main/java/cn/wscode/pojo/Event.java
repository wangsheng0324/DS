package cn.wscode.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * @author wangSheng
 * @title: Event
 * @projectName DS
 * @description: TODO
 * @date 2022/3/28 23:10
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Event {
    public String name;
    public String url;
    public Long timestamp;

    @Override
    public String toString() {
        return "Event{" +
                "name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
