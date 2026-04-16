package com.example.ecommerceanalytics.service;

import com.example.ecommerceanalytics.entity.DailyTopProduct;
import com.example.ecommerceanalytics.repository.DailyTopProductRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ReportService {

    private final DailyTopProductRepository dailyTopProductRepository;

    public List<DailyTopProduct> getTopProducts(LocalDate reportDate) {
        return dailyTopProductRepository.findByReportDateOrderByRankNoAsc(reportDate);
    }
}