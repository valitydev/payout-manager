package dev.vality.payout.manager.config;

import dev.vality.testcontainers.annotations.KafkaSpringBootTest;
import dev.vality.testcontainers.annotations.kafka.KafkaTestcontainerSingleton;
import dev.vality.testcontainers.annotations.postgresql.PostgresqlTestcontainerSingleton;
import org.springframework.context.annotation.Import;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@PostgresqlTestcontainerSingleton
@KafkaTestcontainerSingleton(
        properties = {
                "kafka.topic.pm-events-payout.produce.enabled=true",
                "kafka.topic.source.consume.enabled=true"},
        topicsKeys = {
                "kafka.topic.pm-events-payout.name",
                "kafka.topic.source.name"})
@KafkaSpringBootTest
@Import(KafkaConsumerConfig.class)
public @interface KafkaPostgresqlSpringBootITest {
}