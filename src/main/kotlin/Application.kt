
import org.apache.kafka.clients.consumer.*
import java.util.*
import org.apache.kafka.common.TopicPartition
import java.util.Arrays
import org.apache.kafka.clients.producer.*


fun main(args: Array<String>) {
    val topic = "test1"
    val topic_reply = "test1-reply"
    val consumerConfig = Properties()
//    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "chatmedev.netzme.id:9092")
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.10.101:9092")
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1")
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    consumerConfig.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer = KafkaConsumer<String, String>(consumerConfig)
    val rebalanceListener = TestConsumerRebalanceListener()

    /*
        Properties for reply
        BEGIN
     */
    val props_Reply = Properties()
    props_Reply.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.10.101:9092")
    props_Reply.put(ProducerConfig.ACKS_CONFIG, "all")
    props_Reply.put(ProducerConfig.RETRIES_CONFIG, 0)
    props_Reply.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props_Reply.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer = KafkaProducer<String, String>(props_Reply)
    val callback = TestCallback()
    val rnd = Random()
    /*
        END
     */

    val assign = false
    if (assign) {
        val tp = TopicPartition(topic, 0)
        val tps = Arrays.asList(tp)
        consumer.assign(tps)
        consumer.seekToBeginning(tps)
    } else {
        consumer.subscribe(Collections.singletonList(topic), rebalanceListener)
    }

    while (true) {
        val records: ConsumerRecords<String, String> = consumer.poll(1000)
        for (record: ConsumerRecord<String, String> in records) {
            System.out.printf("Received Message topic =%s, partition =%s, offset = %d, key = %s, value = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value())
            val data = ProducerRecord(
                    topic_reply, record.key(), "reply of " + record.value() + " ")
            producer.send(data, callback)
            producer.close()
        }
        consumer.commitSync()
    }
}

internal class TestConsumerRebalanceListener: ConsumerRebalanceListener
{
    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
        println("Called onPartitionsRevoked with partitions:" + partitions)
    }

    override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
        println("Called onPartitionsAssigned with partitions:" + partitions)
    }
}

internal class TestCallback: Callback {
    override fun onCompletion(metadata: RecordMetadata?, exception: Exception?) {
        if (exception != null) {
            System.out.println("Error while producing message to topic :" + metadata)
            exception.printStackTrace()
        } else {
            val message = String.format("sent message to topic:%s partition:%s  offset:%s", metadata?.topic(), metadata?.partition(), metadata?.offset())
            println(message)
        }
    }

}