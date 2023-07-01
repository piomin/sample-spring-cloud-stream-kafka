package pl.piomin.samples.kafka.stock.model;

public class OrdersSellBuy {

    private int sellCount;
    private int buyCount;

    public OrdersSellBuy() {
    }

    public OrdersSellBuy(int sellCount, int buyCount) {
        this.sellCount = sellCount;
        this.buyCount = buyCount;
    }

    public OrdersSellBuy addSell(int sellCount) {
        this.sellCount += sellCount;
        return this;
    }

    public OrdersSellBuy addBuy(int buyCount) {
        this.buyCount += buyCount;
        return this;
    }

    public int getSellCount() {
        return sellCount;
    }

    public void setSellCount(int sellCount) {
        this.sellCount = sellCount;
    }

    public int getBuyCount() {
        return buyCount;
    }

    public void setBuyCount(int buyCount) {
        this.buyCount = buyCount;
    }

    @Override
    public String toString() {
        return "OrdersSellBuy{" +
                "sellCount=" + sellCount +
                ", buyCount=" + buyCount +
                '}';
    }
}
