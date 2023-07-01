package pl.piomin.samples.kafka.stock.model;

public class TransactionTotal {
    private int count;
    private int productCount;
    private long amount;

    public TransactionTotal() {
    }

    public TransactionTotal(int count, int productCount, long amount) {
        this.count = count;
        this.productCount = productCount;
        this.amount = amount;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getProductCount() {
        return productCount;
    }

    public void setProductCount(int productCount) {
        this.productCount = productCount;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "TransactionTotal{" +
                "count=" + count +
                ", productCount=" + productCount +
                ", amount=" + amount +
                '}';
    }
}
