package com.vivek.platform.order.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Enables the scheduler that drives the transactional outbox relay. */
@Configuration
@EnableScheduling
public class SchedulingConfig {
}
