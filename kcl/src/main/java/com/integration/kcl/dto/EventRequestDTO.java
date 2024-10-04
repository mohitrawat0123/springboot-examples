package com.integration.kcl.dto;

import lombok.Data;

import javax.validation.constraints.NotBlank;

/**
 * @author mohit.rawat
 */
@Data
public class EventRequestDTO {
    @NotBlank
    private String partitionKey;

    @NotBlank
    private String data;
}
