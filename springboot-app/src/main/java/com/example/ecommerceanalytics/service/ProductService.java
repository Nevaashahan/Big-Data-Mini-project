package com.example.ecommerceanalytics.service;

import com.example.ecommerceanalytics.entity.Product;
import com.example.ecommerceanalytics.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;

    public List<Product> getAllProducts() {
        return productRepository.findAll();
    }
}