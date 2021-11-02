package pl.piomin.samples.kafka.stock.model;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class OrderStatus {
    private Long id;
    private Integer productId;
    private int productCount;
    private int realizedCount;
    private OrderType type;
}
