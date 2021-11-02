package pl.piomin.samples.kafka.order.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import lombok.*;

import java.time.LocalDateTime;

@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Transaction {
    private Long id;
    private Long buyOrderId;
    private Long sellOrderId;
    private int amount;
    private int price;
    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    private LocalDateTime creationTime;
}
