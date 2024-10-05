package com.integration.sqs.controller;

import com.integration.sqs.dto.MessageRequestDTO;
import com.integration.sqs.producer.EventProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author mohitrawat0123
 */
@RestController
@RequestMapping("/dev")
@RequiredArgsConstructor
public class DevController {

    private final EventProducer eventProducer;

    @PostMapping("/send")
    public ResponseEntity<?> sendMessage(@RequestBody MessageRequestDTO requestDTO) {
        eventProducer.sendMessage(requestDTO.getMessage());
        return ResponseEntity.ok("Message sent to Q.");
    }

    @PostMapping("/publish")
    public ResponseEntity<?> publishMessage(@RequestBody MessageRequestDTO requestDTO) {
        eventProducer.publishMessage(requestDTO.getMessage());
        return ResponseEntity.ok("Message published to topic.");
    }
}

