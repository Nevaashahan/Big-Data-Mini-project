package com.example.ecommerceanalytics.service;

import com.example.ecommerceanalytics.entity.DailyConversionReport;
import com.example.ecommerceanalytics.entity.DailyTopProduct;
import com.example.ecommerceanalytics.repository.DailyConversionReportRepository;
import com.example.ecommerceanalytics.repository.DailyTopProductRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ReportService {

    private final DailyTopProductRepository dailyTopProductRepository;
    private final DailyConversionReportRepository dailyConversionReportRepository;

    public List<DailyTopProduct> getTopProducts(LocalDate reportDate) {
        return dailyTopProductRepository.findByReportDateOrderByRankNoAsc(reportDate);
    }

    public List<DailyConversionReport> getConversionReport(LocalDate reportDate) {
        return dailyConversionReportRepository.findByReportDateOrderByConversionRateDescCategoryAsc(reportDate);
    }
}
