package asymmtric.encryption.client;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.crypto.Cipher;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ClientApplication {

    enum SecretWord {
        APPLE, BANANA, CHERRY, BLUEBERRY, PEAR, LEMON, PEACH, MANGO
    }

    public static void main(String[] args) throws Exception {
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        KafkaProducer<String, String> producer = createKafkaProducer();

        String checkKafka;
        String publicKeyBase64;

        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PublicKey publicKey = null;

        while (true) {
            System.out.println("Waiting for message with public key");
            publicKeyBase64 = getPublicKeyFromKafka(consumer);
            if (publicKeyBase64 != null) {
                System.out.println("New public key");
                publicKey = transformFromStringToPublicKey(publicKeyBase64, keyFactory);
                break;
            }
        }

        while (true) {
            try {
                // Получение публичного ключа
                checkKafka = getPublicKeyFromKafka(consumer);
                if (checkKafka != null) {
                    System.out.println("New public key");
                    publicKeyBase64 = checkKafka;
                    publicKey = transformFromStringToPublicKey(publicKeyBase64, keyFactory);
                }

                // Шифрование и отправка сообщений
                for (int i = 0; i < 3; i++) {
                    String word = SecretWord.values()[(int) (Math.random() * SecretWord.values().length)].name(); // Случайное слово из Enum
                    System.out.println("Encrypting and sending: " + word);

                    // Шифрование данных
                    Cipher cipher = Cipher.getInstance("RSA");
                    cipher.init(Cipher.ENCRYPT_MODE, publicKey);
                    byte[] encryptedData = cipher.doFinal(word.getBytes());

                    // Отправка зашифрованных данных
                    producer.send(new ProducerRecord<>("encrypted-data-topic", "encrypted-data", Base64.getEncoder().encodeToString(encryptedData)));

                    // Задержка перед следующим сообщением
                    TimeUnit.SECONDS.sleep((i + 1) * 5);
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    private static PublicKey transformFromStringToPublicKey(String publicKeyBase64, KeyFactory keyFactory) throws InvalidKeySpecException {
        // Преобразование публичного ключа из Base64
        byte[] decodedKey = Base64.getDecoder().decode(publicKeyBase64);
        return keyFactory.generatePublic(new java.security.spec.X509EncodedKeySpec(decodedKey));
    }

    private static String getPublicKeyFromKafka(KafkaConsumer<String, String> consumer) {
        String latestValue = null;

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
        for (ConsumerRecord<String, String> record : records) {
            latestValue = record.value(); // Просто запоминаем последнее
            System.out.println("Received public key from Kafka: " + latestValue);
        }

        // Если ничего не пришло, latestValue будет null — обработать на стороне вызывающего кода
        return latestValue;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "server-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("public-key-topic"));
        return consumer;
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }
}