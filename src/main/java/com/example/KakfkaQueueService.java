package com.example;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

public class KakfkaQueueService implements QueueService{
    private static final Properties properties ;

    static{
        properties = new Properties();
        String propFileName = "kafkaconfig.properties";
        try (InputStream inStream = KakfkaQueueService.class.getClassLoader().getResourceAsStream(propFileName)) {
            properties.load(inStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // TODO : Make this fucntion into seperate function for S in Solid
    @Override
    public void push(String queueUrl, String messageBody)  {

        if(messageBody == null || messageBody.isBlank() || queueUrl == null || queueUrl.isBlank())
            throw new RuntimeException("Message Body or URL cannot be empty");

        // Open a connection
        HttpURLConnection connection;
        try {
            connection = (HttpURLConnection) new URL(queueUrl).openConnection();

            // Set the request method to POST
            connection.setRequestMethod("GET");

            String authHeader ="Basic " + properties.getProperty("basic.auth.user.info");
            connection.setRequestProperty("Authorization", authHeader);

            // Enable input/output streams for the connection
            connection.setDoOutput(true);
            connection.setDoInput(true);

            // Write the request body to the output stream
            try (DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
                wr.writeBytes(messageBody);
                wr.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
            // Read the response using try-with-resources
            try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                String inputLine;
                StringBuilder response = new StringBuilder();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }

                // Print the response
                System.out.println("Response: " + response);
            }
            //TODO : Remove this Testing purpose
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Message pull(String queueUrl) {
        if(queueUrl == null || queueUrl.isBlank()){
            throw new RuntimeException("queueURL cannot be blank");
        }
        // Open a connection
        HttpURLConnection connection;
        try {
            connection = (HttpURLConnection) new URL(queueUrl).openConnection();

            // Set the request method to POST
            connection.setRequestMethod("GET");

            String authHeader ="Basic " + properties.getProperty("basic.auth.user.info");
            connection.setRequestProperty("Authorization", authHeader);
//            connection.setRequestProperty("Kafka-Auto-Offset-Reset" , "earliest");

            // Enable input/output streams for the connection
            connection.setDoInput(true);

            // Read the response using try-with-resources
            try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                String inputLine;
                StringBuilder response = new StringBuilder();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }

//                // Print the response
//                System.out.println("Response: " + response);
                return new Message(response.toString());
            }
            //TODO : Remove this Testing purpose
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(String queueUrl, String receiptId) {

    }
}
