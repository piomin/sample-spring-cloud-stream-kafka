## Task 1

Go to the `pl.piomin.samples.kafka.stock.StockService` class. Currently, there is only a single method for creating transactions from orders. Add all required beans there. \
Go to the `application.yml` file to provide configuration settings.

In the `application.yml` file you need to add the following property for each function definition. It is required if you have more than one function definition:
```yaml
spring.cloud.stream.kafka.streams.binder.functions.<function_name>.applicationId: <unique_name>
```

After creating Kafka instance on cloud.redhat.com add privileges for to the topics and consumer groups to your account. \
In both cases set `is * Allow All`.

To run Spring Boot application use the `mvn clean spring-boot:run` command.

## Task 2

If you add the second Spring Cloud Function bean (`Consumer`, `Function`, `Supplier`) you need the following property in the `application.yml` file:
```yaml
spring.cloud.stream.function.definition: <function-1-name>;<function-2-name>
```

When joining two streams you also need to declare `Serdes` (Serializers / Deserializers) for both keys and values. \
For simple types like `int`, `long` or `String` you can the static types defined in the `Serdes` class, e.g. `Serdes.Long()`. \
For objects (deserialized form JSON) use `JsonSerde` object, e.g. `new JsonSerde<>(Order.class)`.

For defining join windows use `JoinWindows.of(...)` method. \
For defining `Serdes` use `StreamJoined.with(...)` method.

## Task 3

The method responsible for updating the status of `Order` in the database is available in the `pl.piomin.samples.kafka.stock.logic.OrderLogic` class. \
It should be used in the `pl.piomin.samples.kafka.stock.StockService` `execute` method. \

If you don't want to create a new object to the stream just return `null`. Then you can filter out `null` objects by using `filterNot(...)` method on the `KStream`.

## Task 4

In order to create persistent store you can use static method `Stores.persistentKeyValueStore(...)` passing the name of the store. It returns `StoreSupplier` specific class. \
When calling grouping method the same as for joins you need to pass `Serdes`. For grouping operations you can use `Grouped.with(...)`. \
For aggregations you need to set Materialized view. Use `Materialized.as(...)` passing already created `StoreSupplier`.

## Task 5

In comparison to the previous task you should create a different type os store dedicated for windowed operations. \
Use `Stores.persistentWindowStore(...)` method for that.