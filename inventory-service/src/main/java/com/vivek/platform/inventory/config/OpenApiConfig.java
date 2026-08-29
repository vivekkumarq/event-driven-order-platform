package com.vivek.platform.inventory.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI inventoryServiceOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Inventory Service API")
                .version("0.0.1")
                .description("""
                        Owns stock levels per SKU and participates in the order saga: it reserves
                        stock when an order is created, publishes the outcome back to order-service,
                        and releases the units again when an order is cancelled.""")
                .contact(new Contact().name("Vivek Kumar"))
                .license(new License().name("MIT").url("https://opensource.org/licenses/MIT")));
    }
}
