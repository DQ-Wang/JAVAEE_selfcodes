package cn.edu.xmu.javaee.productdemoredis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"cn.edu.xmu.javaee.core", "cn.edu.xmu.javaee.productdemoredis"})
public class ProductDemoRedisApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProductDemoRedisApplication.class, args);
    }

}
