package pl.piomin.samples.kafka.stock;

import org.springframework.boot.SpringApplication;

public class TestStockService {

    public static void main(String[] args) {
        SpringApplication.from(StockService::main)
                .with(KafkaContainerDevMode.class)
                .run(args);
    }

}
