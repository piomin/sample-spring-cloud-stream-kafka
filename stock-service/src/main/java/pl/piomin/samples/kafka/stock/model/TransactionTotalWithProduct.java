package pl.piomin.samples.kafka.stock.model;

public class TransactionTotalWithProduct {
    private Transaction transaction;
    private Integer productId;

    public TransactionTotalWithProduct() {
    }

    public TransactionTotalWithProduct(Transaction transaction, Integer productId) {
        this.transaction = transaction;
        this.productId = productId;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public Integer getProductId() {
        return productId;
    }

    public void setProductId(Integer productId) {
        this.productId = productId;
    }

    @Override
    public String toString() {
        return "TransactionTotalWithProduct{" +
                "transaction=" + transaction +
                ", productId=" + productId +
                '}';
    }
}
