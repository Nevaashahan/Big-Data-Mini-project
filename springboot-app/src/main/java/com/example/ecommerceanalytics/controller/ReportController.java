package com.example.ecommerceanalytics.controller;

import com.example.ecommerceanalytics.entity.DailyConversionReport;
import com.example.ecommerceanalytics.entity.DailyTopProduct;
import com.example.ecommerceanalytics.service.ReportService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.List;

@RestController
@RequestMapping("/reports")
@RequiredArgsConstructor
@Tag(name = "Reports", description = "APIs for daily analytics reports")
public class ReportController {

    private final ReportService reportService;

    @GetMapping("/top-products")
    @Operation(summary = "Get top products by date", description = "Returns ranked top viewed products for a given report date")
    public List<DailyTopProduct> getTopProducts(
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate reportDate
    ) {
        LocalDate effectiveDate = reportDate != null ? reportDate : LocalDate.now();
        return reportService.getTopProducts(effectiveDate);
    }

    @GetMapping("/conversion")
    @Operation(summary = "Get conversion report by date", description = "Returns category-level conversion metrics for a given report date")
    public List<DailyConversionReport> getConversionReport(
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate reportDate
    ) {
        LocalDate effectiveDate = reportDate != null ? reportDate : LocalDate.now();
        return reportService.getConversionReport(effectiveDate);
    }
}
