package com.example.ecommerceanalytics.repository;

import com.example.ecommerceanalytics.entity.FlashSaleAlert;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface FlashSaleAlertRepository extends JpaRepository<FlashSaleAlert, Long> {

    List<FlashSaleAlert> findByProductIdOrderByWindowStartDesc(String productId);
}