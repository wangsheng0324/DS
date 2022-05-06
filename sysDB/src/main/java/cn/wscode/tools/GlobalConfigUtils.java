package cn.wscode.tools;

import java.util.Locale;
import java.util.ResourceBundle;

/**
 * Created by angel
 */
public class GlobalConfigUtils {
    static ResourceBundle bundle = ResourceBundle.getBundle("application" , Locale.ENGLISH);
    static public String host = bundle.getString("canal.host").trim();
    static public String port = bundle.getString("canal.port").trim();
    static public String  instance = bundle.getString("canal.instance").trim();
    static public String  username = bundle.getString("mysql.username").trim();
    static public String  password = bundle.getString("mysql.password").trim();

    public static void main(String[] args) {
        System.out.println(host+"--"+port+"--"+instance+"---"+username+"---"+password);
    }
}
