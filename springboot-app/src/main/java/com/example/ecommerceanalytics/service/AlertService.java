package com.example.ecommerceanalytics.service;

import com.example.ecommerceanalytics.entity.FlashSaleAlert;
import com.example.ecommerceanalytics.repository.FlashSaleAlertRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class AlertService {

    private final FlashSaleAlertRepository flashSaleAlertRepository;

    public List<FlashSaleAlert> getAllAlerts() {
        return flashSaleAlertRepository.findAll();
    }

    public List<FlashSaleAlert> getAlertsByProductId(String productId) {
        return flashSaleAlertRepository.findByProductIdOrderByWindowStartDesc(productId);
    }
}