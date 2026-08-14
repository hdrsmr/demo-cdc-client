package com.abc.hazelcast.cdc.dto;



import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Page Response")
public class PageResponse<T> {

    @Schema(description = "List of data")
    private List<T> content;

    @Schema(description = "Current page number", example = "0")
    private int pageNumber;

    @Schema(description = "Page size", example = "10")
    private int pageSize;

    @Schema(description = "Total elements", example = "100")
    private long totalElements;

    @Schema(description = "Total pages", example = "10")
    private int totalPages;

    @Schema(description = "Is last page", example = "false")
    private boolean last;
}