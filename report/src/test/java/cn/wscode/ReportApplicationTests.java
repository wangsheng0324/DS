package cn.wscode;

import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Random;

@SpringBootTest
class ReportApplicationTests {
    public static void main(String[] args) {
        Random random = new Random();
        for (int i = 0; i < 10; i++) {

            System.out.println(random.nextInt(13));

        }

    }

}
