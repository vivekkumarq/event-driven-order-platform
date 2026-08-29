package com.vivek.platform.order.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI orderServiceOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Order Service API")
                .version("0.0.1")
                .description("""
                        Accepts orders, drives the order lifecycle and emits domain events through a
                        transactional outbox. Orders start PENDING and are moved to CONFIRMED or
                        REJECTED by the reservation result published by inventory-service.""")
                .contact(new Contact().name("Vivek Kumar"))
                .license(new License().name("MIT").url("https://opensource.org/licenses/MIT")));
    }
}
