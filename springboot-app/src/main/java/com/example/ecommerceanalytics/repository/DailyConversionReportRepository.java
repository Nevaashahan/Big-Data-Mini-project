package com.example.ecommerceanalytics.repository;

import com.example.ecommerceanalytics.entity.DailyConversionReport;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDate;
import java.util.List;

public interface DailyConversionReportRepository extends JpaRepository<DailyConversionReport, Long> {

    List<DailyConversionReport> findByReportDateOrderByConversionRateDescCategoryAsc(LocalDate reportDate);
}
