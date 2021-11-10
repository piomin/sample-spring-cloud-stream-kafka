package pl.piomin.samples.kafka.stock.controller;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import pl.piomin.samples.kafka.stock.model.TransactionTotal;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/transactions")
public class TransactionController {

    private InteractiveQueryService queryService;

    public TransactionController(InteractiveQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/all")
    public TransactionTotal getAllTransactionsSummary() {
        ReadOnlyKeyValueStore<String, TransactionTotal> keyValueStore =
                queryService.getQueryableStore("all-transactions-store",
                        QueryableStoreTypes.keyValueStore());
        return keyValueStore.get("NEW");
    }

    @GetMapping("/product/{productId}")
    public TransactionTotal getSummaryByProductId(@PathVariable("productId") Integer productId) {
        ReadOnlyKeyValueStore<Integer, TransactionTotal> keyValueStore =
                queryService.getQueryableStore("transactions-per-product-store",
                        QueryableStoreTypes.keyValueStore());
        return keyValueStore.get(productId);
    }

    @GetMapping("/product/latest/{productId}")
    public TransactionTotal getLatestSummaryByProductId(@PathVariable("productId") Integer productId) {
        ReadOnlyKeyValueStore<Integer, TransactionTotal> keyValueStore =
                queryService.getQueryableStore("latest-transactions-per-product-store",
                        QueryableStoreTypes.keyValueStore());
        return keyValueStore.get(productId);
    }

    @GetMapping("/product")
    public Map<Integer, TransactionTotal> getSummaryByAllProducts() {
        Map<Integer, TransactionTotal> m = new HashMap<>();
        ReadOnlyKeyValueStore<Integer, TransactionTotal> keyValueStore =
                queryService.getQueryableStore("transactions-per-product-store",
                        QueryableStoreTypes.keyValueStore());
        KeyValueIterator<Integer, TransactionTotal> it = keyValueStore.all();
        while (it.hasNext()) {
            KeyValue<Integer, TransactionTotal> kv = it.next();
            m.put(kv.key, kv.value);
        }
        return m;
    }

}
