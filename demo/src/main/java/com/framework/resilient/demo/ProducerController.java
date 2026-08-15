package com.framework.resilient.demo;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * REST controller exposing endpoints for demo event production scenarios.
 */
@RestController
@RequestMapping("/produce")
@Validated
public class ProducerController {

    private final PaymentEventProducer producer;

    public ProducerController(PaymentEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/sequenced")
    public ResponseEntity<Map<String, Object>> produceSequenced(
            @RequestParam @NotBlank String accountId,
            @RequestParam @Min(1) @Max(10000) int count) {
        producer.produceSequenced(accountId, count);
        return ResponseEntity.ok(Map.of(
                "status", "produced",
                "scenario", "sequenced",
                "accountId", accountId,
                "count", count
        ));
    }

    @PostMapping("/out-of-order")
    public ResponseEntity<Map<String, Object>> produceOutOfOrder(
            @RequestParam @NotBlank String accountId,
            @RequestParam @Min(1) @Max(10000) int count) {
        producer.produceOutOfOrder(accountId, count);
        return ResponseEntity.ok(Map.of(
                "status", "produced",
                "scenario", "out-of-order",
                "accountId", accountId,
                "count", count
        ));
    }

    @PostMapping("/duplicates")
    public ResponseEntity<Map<String, Object>> produceDuplicates(
            @RequestParam @NotBlank String accountId,
            @RequestParam long seq) {
        producer.produceDuplicates(accountId, seq);
        return ResponseEntity.ok(Map.of(
                "status", "produced",
                "scenario", "duplicates",
                "accountId", accountId,
                "sequenceNumber", seq
        ));
    }

    @PostMapping("/poison-pill")
    public ResponseEntity<Map<String, Object>> producePoisonPill() {
        producer.producePoisonPill();
        return ResponseEntity.ok(Map.of(
                "status", "produced",
                "scenario", "poison-pill"
        ));
    }
}
