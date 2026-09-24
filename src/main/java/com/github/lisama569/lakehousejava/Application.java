package com.github.lisama569.lakehousejava;

import com.github.lisama569.lakehousejava.table.LocalLakehouseDemo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.nio.file.Path;

@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        if (args.length > 0 && "demo".equals(args[0])) {
            LocalLakehouseDemo.run(Path.of(args.length > 1 ? args[1] : "lakehouse-demo-data"));
            return;
        }
        SpringApplication.run(Application.class, args);
    }
}
