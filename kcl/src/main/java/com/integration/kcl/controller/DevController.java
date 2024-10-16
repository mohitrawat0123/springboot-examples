package com.integration.kcl.controller;

import com.integration.kcl.dto.EventRequestDTO;
import com.integration.kcl.producer.KinesisProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

/**
 * @author mohit.rawat
 */
@Log4j2
@RestController
@RequestMapping("/dev")
@RequiredArgsConstructor
public class DevController {

    private final KinesisProducer kinesisProducer;

    @PostMapping("/send")
    public ResponseEntity<?> addEvent(@Valid @RequestBody EventRequestDTO requestDTO) {
        kinesisProducer.addEvent(requestDTO.getPartitionKey(), requestDTO.getData());
        return ResponseEntity.ok("Event sent successfully.");
    }

}
