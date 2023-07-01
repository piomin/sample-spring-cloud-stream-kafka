package pl.piomin.samples.kafka.stock.repository;

import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.repository.CrudRepository;
import pl.piomin.samples.kafka.stock.model.Order;

import jakarta.persistence.LockModeType;
import java.util.Optional;

public interface OrderRepository extends CrudRepository<Order, Long> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<Order> findById(Long id);

}
