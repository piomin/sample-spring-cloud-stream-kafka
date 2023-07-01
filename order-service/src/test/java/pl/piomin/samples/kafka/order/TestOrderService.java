package pl.piomin.samples.kafka.order;

import org.springframework.boot.SpringApplication;

public class TestOrderService {

    public static void main(String[] args) {
        SpringApplication.from(OrderService::main)
                .with(KafkaContainerDevMode.class)
                .run(args);
    }

}
