package org.example;

//apache flink imports

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

public class App {

        public static void main(String[] args) throws Exception {
            // Set Kafka bootstrap servers
            String bootstrapServers = "kafka:29092";

            //Kafka inputTopic name to read from
            String inputTopic = "flink_input_topic";
            String outputTopic = "flink_output_topic";

            //Properties for Kafka consumer and producer
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", bootstrapServers);
            properties.setProperty("key.serializer", StringSerializer.class.getName());
            properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            properties.setProperty("request.timeout.ms", "5000"); // Adjust the value as needed

            // Create execution environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // Create Kafka consumer
            FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(inputTopic, new SimpleStringSchema(), properties);
            consumer.setStartFromEarliest();

            // Create Kafka producer
            FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(outputTopic, new SimpleStringSchema(), properties);

            // Create stream from Kafka consumer
            DataStream<String> stream = env.addSource(consumer);

            // Map stream to SaleEvent
            DataStream<SaleEvent> saleEventStream = stream.map(new MapFunction<String, SaleEvent>() {

                public SaleEvent map(String s) throws Exception {
                    ObjectMapper objectMapper = new ObjectMapper();
                    return objectMapper.readValue(s, SaleEvent.class);
                }
            });

            // Filter stream to only include amountDinar for sales from seller1
            DataStream<SaleEvent> filteredStream = saleEventStream.map(new MapFunction<SaleEvent, SaleEvent>() {

                public SaleEvent map(SaleEvent saleEvent) throws Exception {
                    // Check if the seller is "seller1"
                    if (saleEvent.getSellerId().equals("seller1")) {
                        // Enrich data for seller1 by adding the usdToDinar field
                        saleEvent.setAmountDinar(getDollarRateForTND());
                    } else {
                        // Set usdToDinar to null for all other sellers
                        saleEvent.setAmountDinar(null);
                    }
                    return saleEvent;
                }
            });


            // Map stream to string
            DataStream<String> stringStream = filteredStream.map(new MapFunction<SaleEvent, String>() {
                public String map(SaleEvent saleEvent) throws Exception {
                    return saleEvent.toJson();
                }
            });

            // Write stream to Kafka producer
            stringStream.addSink(producer);

            // Execute
            env.execute("Flink Streaming Java API Skeleton USD to TND Conversion for seller1 only App");
        }



    public static Double getDollarRateForTND() {

        Double usdToDinarRate = null;

        try {
            // Replace with your actual API key and endpoint
            String apiKey = "1705a49c2d4a22a8ba698b07";
            String endpoint = "https://v6.exchangerate-api.com/v6/1705a49c2d4a22a8ba698b07/latest/USD";

            // Create URL object
            URL url = new URL(endpoint);

            // Open connection
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            // Set request method
            connection.setRequestMethod("GET");

            // Get response code
            int responseCode = connection.getResponseCode();

            if (responseCode == HttpURLConnection.HTTP_OK) {
                // Read response
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                StringBuilder response = new StringBuilder();
                String line;

                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }

                reader.close();

                // Parse the JSON response
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode jsonResponse = objectMapper.readTree(response.toString());

                // Extract the USD to Dinar exchange rate
                 usdToDinarRate = jsonResponse
                        .path("conversion_rates")
                        .path("TND")
                        .asDouble();

                System.out.println("USD to Dinar Exchange Rate: " + usdToDinarRate);

            } else {
                System.out.println("API request failed. Response Code: " + responseCode);

            }

            // Close connection
            connection.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return usdToDinarRate;
    }

}
