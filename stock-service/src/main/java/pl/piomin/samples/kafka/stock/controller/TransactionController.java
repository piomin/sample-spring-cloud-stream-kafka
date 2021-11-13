package pl.piomin.samples.kafka.stock.controller;

import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/transactions")
public class TransactionController {

    private InteractiveQueryService queryService;

    public TransactionController(InteractiveQueryService queryService) {
        this.queryService = queryService;
    }

}
