package pl.piomin.samples.kafka.stock;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import pl.piomin.samples.kafka.stock.logic.OrderLogic;
import pl.piomin.samples.kafka.stock.model.Order;
import pl.piomin.samples.kafka.stock.model.Transaction;

import java.time.LocalDateTime;

@SpringBootApplication
@Slf4j
public class StockService {

    private static long transactionId = 0;

    public static void main(String[] args) {
        SpringApplication.run(StockService.class, args);
    }

    @Autowired
    OrderLogic logic;



    private Transaction execute(Order orderBuy, Order orderSell) {
        if (orderBuy.getAmount() >= orderSell.getAmount()) {
            return new Transaction(
                ++transactionId,
                orderBuy.getId(),
                orderSell.getId(),
                Math.min(orderBuy.getProductCount(), orderSell.getProductCount()),
                (orderBuy.getAmount() + orderSell.getAmount()) / 2,
                LocalDateTime.now(),
                "NEW");
        } else {
            return null;
        }
    }

}
