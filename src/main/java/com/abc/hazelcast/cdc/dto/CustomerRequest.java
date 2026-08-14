package com.abc.hazelcast.cdc.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Customer Request DTO")
public class CustomerRequest {

    @Schema(description = "Customer Code", example = "CUST001", required = true)
    @NotBlank(message = "Customer code is required")
    @Size(max = 20, message = "Customer code max 20 characters")
    private String customerCode;

    @Schema(description = "Customer Name", example = "John Doe", required = true)
    @NotBlank(message = "Customer name is required")
    @Size(max = 100, message = "Customer name max 100 characters")
    private String customerName;

    @Schema(description = "Email", example = "john@example.com")
    @Email(message = "Invalid email format")
    @Size(max = 100, message = "Email max 100 characters")
    private String email;

    @Schema(description = "City", example = "Jakarta")
    @Size(max = 50, message = "City max 50 characters")
    private String city;
}