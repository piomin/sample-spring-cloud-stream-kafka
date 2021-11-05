package pl.piomin.samples.kafka.stock.controller;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import pl.piomin.samples.kafka.stock.model.Order;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/orders-status")
public class OrderStatusController {

    private InteractiveQueryService queryService;

    public OrderStatusController(InteractiveQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping
    public List<Order> getOrders() {
        List<Order> orders = new ArrayList<>();
        ReadOnlyKeyValueStore<Long, Order> keyValueStore =
                queryService.getQueryableStore("orders-status", QueryableStoreTypes.keyValueStore());
        KeyValueIterator<Long, Order> it = keyValueStore.all();
        while (it.hasNext()) {
            KeyValue<Long, Order> kv = it.next();
            orders.add(kv.value);
        }
        return orders;
    }
}
