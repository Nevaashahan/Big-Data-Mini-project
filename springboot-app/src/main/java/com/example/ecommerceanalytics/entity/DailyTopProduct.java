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

import java.time.LocalDate;

@Entity
@Table(name = "daily_top_products")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DailyTopProduct {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "report_date", nullable = false)
    private LocalDate reportDate;

    @Column(name = "rank_no", nullable = false)
    private Integer rankNo;

    @Column(name = "product_id", nullable = false)
    private String productId;

    @Column(name = "total_views", nullable = false)
    private Integer totalViews;
}