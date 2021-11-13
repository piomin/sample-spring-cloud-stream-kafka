## Task 1

Go to the `pl.piomin.samples.kafka.stock.StockService` class. Currently, there is only a single method for creating transactions from orders. Add all required beans there. \
Go to the `application.yml` file to provide configuration settings.

In the `application.yml` file you need to add the following property for each function definition. It is required if you have more than one function definition:
```yaml
spring.cloud.stream.kafka.streams.binder.functions.<function_name>.applicationId: <unique_name>
```

## Task 2

If you add the second Spring Cloud Function bean (`Consumer`, `Function`, `Supplier`) you need the following property in the `application.yml` file:
```yaml
spring.cloud.stream.function.definition: <function-1-name>;<function-2-name>
```

## Task 3

The method responsible for updating the status of `Order` in the database is available on `pl.piomin.samples.kafka.stock.logic.OrderLogic`