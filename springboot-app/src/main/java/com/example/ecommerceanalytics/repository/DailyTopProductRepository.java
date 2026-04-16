package com.example.ecommerceanalytics.repository;

import com.example.ecommerceanalytics.entity.DailyTopProduct;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDate;
import java.util.List;

public interface DailyTopProductRepository extends JpaRepository<DailyTopProduct, Long> {

    List<DailyTopProduct> findByReportDateOrderByRankNoAsc(LocalDate reportDate);
}