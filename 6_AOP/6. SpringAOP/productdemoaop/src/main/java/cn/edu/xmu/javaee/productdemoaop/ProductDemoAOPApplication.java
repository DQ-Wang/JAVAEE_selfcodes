package cn.edu.xmu.javaee.productdemoaop;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"cn.edu.xmu.javaee.core", "cn.edu.xmu.javaee.productdemoaop"})
public class ProductDemoAOPApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProductDemoAOPApplication.class, args);
    }

}
