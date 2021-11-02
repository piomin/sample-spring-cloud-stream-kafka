package pl.piomin.samples.kafka.stock.model;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class OrdersSellBuy {
    private int sellCount;
    private int buyCount;

    public OrdersSellBuy addSell(int sellCount) {
        this.sellCount += sellCount;
        return this;
    }

    public OrdersSellBuy addBuy(int buyCount) {
        this.buyCount += buyCount;
        return this;
    }
}
