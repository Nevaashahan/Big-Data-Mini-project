package com.example.ecommerceanalytics.controller;

import com.example.ecommerceanalytics.entity.Product;
import com.example.ecommerceanalytics.service.ProductService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/products")
@RequiredArgsConstructor
@Tag(name = "Products", description = "APIs for product data")
public class ProductController {

    private final ProductService productService;

    @GetMapping
    @Operation(summary = "Get all products", description = "Returns all products from the PostgreSQL database")
    public List<Product> getAllProducts() {
        return productService.getAllProducts();
    }
}