package asymmtric.encryption.server;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ServerApplication {
    public static void main(String[] args) throws Exception {
        //long lastExecutionTime = System.currentTimeMillis() - 20000;
        long lastExecutionTime = System.currentTimeMillis();
        long interval = 30000; // 60 sec
        long currentTime;
        long deltaTime;

        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        KafkaProducer<String, String> producer = createKafkaProducer();

        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        KeyPair keyPair = null;
        PublicKey publicKey = null;
        PrivateKey privateKey = null;


        while (true) {
            try {
                currentTime = System.currentTimeMillis();
                deltaTime = currentTime - lastExecutionTime;
                System.out.println("delta: " + deltaTime);

                if (deltaTime > interval) {
                    System.out.println("Starting generate the key pair");
                    keyPair = keyGen.generateKeyPair();
                    publicKey = keyPair.getPublic();
                    privateKey = keyPair.getPrivate();

                    String publicKeyBase64 = java.util.Base64.getEncoder().encodeToString(publicKey.getEncoded());

                    producer.send(new ProducerRecord<>("public-key-topic", "public-key", publicKeyBase64));
                    //producer.close();

                    System.out.println("Public key sent to Kafka: " + publicKeyBase64);

                    lastExecutionTime = System.currentTimeMillis();
                }

                searchForNewMessages(consumer, privateKey);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    private static void searchForNewMessages(KafkaConsumer<String, String> consumer, PrivateKey privateKey) {
        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                String encryptedDataBase64 = record.value();
                System.out.println("Received encrypted data: " + encryptedDataBase64);

                byte[] encryptedData = java.util.Base64.getDecoder().decode(encryptedDataBase64);
                javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("RSA");
                cipher.init(javax.crypto.Cipher.DECRYPT_MODE, privateKey);

                byte[] decryptedData = cipher.doFinal(encryptedData);
                System.out.println("Decrypted data: " + new String(decryptedData));
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "server-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("encrypted-data-topic"));
        return consumer;
    }
}
