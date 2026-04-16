package com.example.ecommerceanalytics.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;

@Entity
@Table(name = "daily_conversion_report")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DailyConversionReport {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "report_date", nullable = false)
    private LocalDate reportDate;

    @Column(nullable = false)
    private String category;

    @Column(name = "total_views", nullable = false)
    private Integer totalViews;

    @Column(name = "total_purchases", nullable = false)
    private Integer totalPurchases;

    @Column(name = "conversion_rate", nullable = false, precision = 8, scale = 4)
    private BigDecimal conversionRate;
}
