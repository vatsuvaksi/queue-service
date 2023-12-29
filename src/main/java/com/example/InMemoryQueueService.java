package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryQueueService implements QueueService {
  private final Map<String, Queue<Message>> queues;

  InMemoryQueueService() {
    this.queues = new ConcurrentHashMap<>();
    String propFileName = "config.properties";
    Properties confInfo = new Properties();

    try (InputStream inStream = getClass().getClassLoader().getResourceAsStream(propFileName)) {
      confInfo.load(inStream);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void push(String queueUrl, String msgBody) {
    validateArguments(queueUrl, msgBody);
    int parsedPriority = parseForPriority(msgBody);
    Queue<Message> queue = queues.get(queueUrl);
    if (queue == null) {
      queue = new PriorityQueue<>(Comparator.comparingInt(Message::getPriority));
      queues.put(queueUrl, queue);
    }
    queue.add(new Message(msgBody , parsedPriority));
  }

  private int parseForPriority(String msgBody) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode jsonNode = objectMapper.readTree(msgBody);
      if(!jsonNode.has("priority")){
        throw new RuntimeException("Priority is needed");
      }
      return jsonNode.path("priority").asInt();
    } catch (IOException e) {
      e.printStackTrace();
      return 1; // Default priority for simplicity
    }
  }

  @Override
  public Message pull(String queueUrl) {
    Queue<Message> queue = queues.get(queueUrl);
    if (queue == null || queue.size() == 0) {
      return null;
    }

      Message msg = queue.peek();
      msg.setReceiptId(UUID.randomUUID().toString());
      msg.incrementAttempts();

      return new Message(msg.getBody(), msg.getReceiptId());
  }

  @Override
  public void delete(String queueUrl, String receiptId) {
    Queue<Message> queue = queues.get(queueUrl);

    if (queue != null) {

      Iterator<Message> iterator = queue.iterator();
      while (iterator.hasNext()) {
        Message msg = iterator.next();
        if (msg.getReceiptId().equals(receiptId)) {
          iterator.remove(); // Remove the message from the queue
          break; // Assuming receipt IDs are unique, exit the loop after removal
        }
      }
    }
  }
  private void validateArguments(String... args) {
    for (String arg : args) {
      if (arg == null || arg.isBlank()) {
        throw new RuntimeException("Argument cannot be empty");
      }
    }
  }
}
