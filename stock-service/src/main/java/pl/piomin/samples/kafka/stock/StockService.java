package pl.piomin.samples.kafka.stock;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import pl.piomin.samples.kafka.stock.model.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

@SpringBootApplication
@Slf4j
public class StockService {

    private static long transactionId = 0;

    public static void main(String[] args) {
        SpringApplication.run(StockService.class, args);
    }

//    @Bean
//    public Consumer<Message<Order>> orders() {
//        return order -> log.info("Received: {}", order.getPayload());
//    }

//    @Bean
//    public Consumer<KStream<Long, Order>> orders() {
//        return order -> order
//                .groupBy((k, v) -> v.getCustomerId(), Grouped.with(Serdes.Integer(), new JsonSerde<>(Order.class)))
//                .windowedBy(TimeWindows.of(Duration.ofSeconds(10)))
//                .count()
//                .toStream()
//                .foreach((k, v) -> log.info("Key: {}, Value: {}", k.key(), v));
//    }

    @Bean
    public JsonSerde<Order> orderJsonSerde() {
        return new JsonSerde<>(Order.class);
    }

//    @Bean
//    public BiConsumer<KStream<Long, Order>, KStream<Long, Order>> orders() {
//        return (orderBuy, orderSell) -> orderBuy
//                .join(orderSell, this::execute,
//                        JoinWindows.of(Duration.ofSeconds(10)),
//                        StreamJoined.with(Serdes.Long(), orderJsonSerde(), orderJsonSerde()))
//                .filter((k, v) -> v != null)
//                .map((k, v) -> new KeyValue<>(v.getBuyOrderId(), v))
//                .groupByKey(Grouped.with(Serdes.Long(), new JsonSerde<>(Transaction.class)))
//                .aggregate(() -> 0, (k, v, a) -> a + v.getAmount(), Materialized.with(Serdes.Long(), Serdes.Integer()))
//                .toStream()
//                .foreach((k, v) -> log.info("Order: {}, Sum: {}", k, v));
//    }

//    @Bean
//    public BiFunction<KStream<Long, Order>, KStream<Long, Order>, KStream<Long, Transaction>> orders() {
//        return (orderBuy, orderSell) -> orderBuy.join(orderSell, this::execute,
//                        JoinWindows.of(Duration.ofSeconds(10)),
//                        StreamJoined.with(Serdes.Long(), orderJsonSerde(), orderJsonSerde()))
//                .filter((k, v) -> v != null)
//                .map((k, v) -> new KeyValue<>(v.getBuyOrderId(), v));
//    }

//    @Bean
//    public Consumer<KStream<Long, Transaction>> transactions() {
//        return trx -> trx
//
//                .groupByKey()
//                .aggregate(() -> null, (k, v, a) -> v, Materialized.with(Serdes.Long(), new JsonSerde<>(Transaction.class)))
//                .toStream()
//                .foreach((k, v) -> log.info("Trx: {}", v));
//    }

//    @Bean
//    public BiConsumer<KStream<Long, Order>, KStream<Long, Order>> ordersx() {
//        return (orderBuy, orderSell) -> orderBuy
//                .merge(orderSell)
//                .groupByKey(Grouped.with(Serdes.Long(), orderJsonSerde()))
//                .aggregate(OrdersSellBuy::new,
//                        (k, v, a) -> v.getType() == OrderType.BUY ? a.addBuy(v.getProductCount()) : a.addSell(v.getProductCount()),
//                        Materialized.with(Serdes.Long(), new JsonSerde(OrdersSellBuy.class)))
//                .toStream()
//                .foreach((k, v) -> log.info("Product id={}: {}", k, orderBuy));
//    }

//    @Bean
//    public StoreBuilder xxx() {
//        return Stores.keyValueStoreBuilder(
//                Stores.persistentKeyValueStore("xxx"), Serdes.Long(), new JsonSerde<>(OrderStatus.class));
//    }

    @Bean
    public BiFunction<KStream<Long, Order>, KStream<Long, Order>, KTable<Long, OrderStatus>> orders() {
        return (orderBuy, orderSell) -> orderBuy
                .merge(orderSell)
                .mapValues(o -> new OrderStatus(o.getId(), o.getProductId(), o.getProductCount(), 0, o.getType()))
//                .peek((k, v) -> log.info("Merged: {}", v))
                .toTable();
    }

    @Bean
    public BiFunction<KStream<Long, Order>, KStream<Long, Order>, KStream<Long, Transaction>> pairs() {
        return (orderBuy, orderSell) -> orderBuy
                .selectKey((k, v) -> v.getProductId())
                .join(orderSell.selectKey((k, v) -> v.getProductId()),
                        this::execute,
                        JoinWindows.of(Duration.ofSeconds(10)),
                        StreamJoined.with(Serdes.Integer(), orderJsonSerde(), orderJsonSerde()))
                .filterNot((k, v) -> v == null)
                .map((k, v) -> new KeyValue<>(v.getId(), v));
    }

    @Bean
    public BiFunction<KStream<Long, Transaction>, KTable<Long, OrderStatus>, KStream<Long, Transaction>> transactions() {
        return (trx, order) -> trx.selectKey((k, v) -> v.getBuyOrderId())
                .join(order, this::confirm,
                        Joined.with(Serdes.Long(), new JsonSerde<>(Transaction.class), new JsonSerde<>(OrderStatus.class)))
                .filterNot((k, v) -> v == null)
                .peek((k, v) -> log.info("CONFIRMED: {}", v))
                .selectKey((k, v) -> v.getSellOrderId())
                .join(order, this::finish,
                        Joined.with(Serdes.Long(), new JsonSerde<>(Transaction.class), new JsonSerde<>(OrderStatus.class)))
                .filterNot((k, v) -> v == null)
                .peek((k, v) -> log.info("DONE: {}", v));
    }

//    @Bean
//    public BiFunction<KStream<Long, Transaction>, KTable<Long, OrderStatus>, KStream<Long, Transaction>> transactions2() {
//        return (trx, order) -> trx.selectKey((k, v) -> v.getSellOrderId())
//                .join(order, this::confirm,
//                        Joined.with(Serdes.Long(), new JsonSerde<>(Transaction.class), new JsonSerde<>(OrderStatus.class)))
//                .filterNot((k, v) -> v == null)
////                .peek((k, v) -> log.info("CONFIRMED: {}", v))
//                .selectKey((k, v) -> v.getBuyOrderId())
//                .join(order, this::finish,
//                        Joined.with(Serdes.Long(), new JsonSerde<>(Transaction.class), new JsonSerde<>(OrderStatus.class)))
//                .filterNot((k, v) -> v == null)
//                .peek((k, v) -> log.info("DONE: {}", v));
//    }

    @Bean
    public BiFunction<KStream<Long, Transaction>, KTable<Long, OrderStatus>, KTable<Long, OrderStatus>> updateBuy() {
        return (trx, order) -> trx.selectKey((k, v) -> v.getBuyOrderId())
                .join(order, this::update,
                        Joined.with(Serdes.Long(), new JsonSerde<>(Transaction.class), new JsonSerde<>(OrderStatus.class)))
                .toTable();
    }

    @Bean
    public BiFunction<KStream<Long, Transaction>, KTable<Long, OrderStatus>, KTable<Long, OrderStatus>> updateSell() {
        return (trx, order) -> trx.selectKey((k, v) -> v.getSellOrderId())
                .join(order, this::update,
                        Joined.with(Serdes.Long(), new JsonSerde<>(Transaction.class), new JsonSerde<>(OrderStatus.class)))
                .toTable();
    }

//    @Bean
//    public Consumer<KTable<Long, OrderStatus>> transactions() {
//        return transaction -> transaction.toStream()
//                .foreach((k, v) -> log.info("T: {}", v));
//    }

//    @Bean
//    public BiFunction<KStream<Long, Transaction>, KTable<Long, OrderStatus>, KTable<Long, OrderStatus>> transactions() {
//        return (transaction, order) -> transaction.join(order,
//                (t, o) -> {
//                    if (o.getRealizedCount() + t.getAmount() == o.getProductCount()) {
//                        log.info("Removing: key={}", o.getId());
//                        return null;
//                    } else {
//                        return new OrderStatus(o.getId(), o.getProductId(), o.getProductCount(), o.getRealizedCount() + t.getAmount(), o.getType());
//                    }
//                })
//                .peek((k, v) -> log.info("K->{}, V->{}", k, v))
//                .toTable();
//    }

    private Transaction execute(Order orderBuy, Order orderSell) {
        if (orderBuy.getAmount() >= orderSell.getAmount()) {
//            log.info("Executed: orderBuy={}, orderSell={}", orderBuy.getId(), orderSell.getId());
            return new Transaction(
                    ++transactionId,
                    orderBuy.getId(),
                    orderSell.getId(),
                    Math.min(orderBuy.getProductCount(), orderSell.getProductCount()),
                    (orderBuy.getAmount() + orderSell.getAmount()) / 2,
                    LocalDateTime.now(),
                    "NEW"
            );
        } else {
//            log.info("Not executed: orderBuy={}, orderSell={}", orderBuy.getId(), orderSell.getId());
            return null;
        }
    }

    private Transaction confirm(Transaction transaction, OrderStatus orderStatus) {
//        log.info("Confirming: trx={}, status={}", transaction, orderStatus);
        int count = Math.min(transaction.getAmount(), orderStatus.getProductCount() - orderStatus.getRealizedCount());
        transaction.setStatus("CONFIRMED");
        transaction.setAmount(count);
        return transaction;
    }

    private Transaction finish(Transaction transaction, OrderStatus orderStatus) {
//        log.info("Finishing: trx={}, status={}", transaction, orderStatus);
        int count = Math.min(transaction.getAmount(), orderStatus.getProductCount() - orderStatus.getRealizedCount());
        transaction.setStatus("FINISH");
        transaction.setAmount(count);
        return transaction;
    }

    private OrderStatus update(Transaction transaction, OrderStatus orderStatus) {
        if (orderStatus.getRealizedCount() + transaction.getAmount() == orderStatus.getProductCount()) {
            log.info("Removing: key={}", orderStatus.getId());
            return null;
        } else {
            orderStatus.setRealizedCount(orderStatus.getRealizedCount() + transaction.getAmount());
            log.info("Updating: key={}, currentValue={}", orderStatus.getId(), orderStatus);
            return orderStatus;
        }
    }
}
