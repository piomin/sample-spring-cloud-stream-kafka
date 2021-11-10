package pl.piomin.samples.kafka.stock.model;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class TransactionTotal {
    private int count;
    private int productCount;
    private long amount;
}
