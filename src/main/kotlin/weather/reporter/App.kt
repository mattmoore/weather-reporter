package weather.reporter

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*

fun main() {
    val consumer = createConsumer()
    consumer.subscribe(listOf("metar"))

    while(true) {
        val records = consumer.poll(Duration.ofSeconds(1))
        records.forEach {
            println(it.value())
        }
    }
}

fun createConsumer(): KafkaConsumer<String, String> {
    val props = Properties()
    props["bootstrap.servers"] = "localhost:9092"
    props["group.id"] = "metar-processor"
    props["key.deserializer"] = StringDeserializer::class.java
    props["value.deserializer"] = StringDeserializer::class.java
    return KafkaConsumer(props)
}

