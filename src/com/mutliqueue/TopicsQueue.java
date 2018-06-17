package com.mutliqueue;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TopicsQueue implements TopicsQueueAPI {

    //IF that number has been reach there will be waiting.
    private static final int MAX_MESSAGES_FOR_TOPIC = 99999;

    //This map only locks once two operations touch the same bucket - which is rare in this case.
    private Map<String, Topic> topicMap = new ConcurrentHashMap<>();

    private static final Logger logger = Logger.getLogger(TopicsQueue.class.getName());

    /**
     * A topic has a queue of messages and an operation to perform on them once they are handled.
     */

    //TODO - Add managed blocker? Next version :-)
    class Topic {
        ArrayBlockingQueue<String> messages;
        Function<String, Void> operation;
        String name;

        public Topic(ArrayBlockingQueue<String> q, Function<String, Void> operation, String name) {
            this.name = name;
            this.messages = q;
            this.operation = operation;
        }

        public void offer(String message) {
            try {
                this.messages.put(message);
            } catch (InterruptedException e) {
                logger.log(Level.SEVERE, "Error: ", e);
            }
        }
    }

    @Override
    public Topic getTopicQueue(String key) {
        return topicMap.get(key);
    }


    @Override
    public void removeTopic(String topic) {
        topicMap.remove(topic);
    }

    /**
     *
     * @param topic - A Topic about which there exists a message queue.
     * @param payload - A message
     * @return true - If this is a new topic
     */

    @Override
    public boolean publish(String topic, String payload) {
        boolean newTopic = !topicMap.containsKey(topic);
        if (newTopic) {

            ArrayBlockingQueue<String> q = new ArrayBlockingQueue<>(MAX_MESSAGES_FOR_TOPIC, true);

            Function<String, Void> operation = s -> {
                logger.log(Level.INFO, " - Handling message: " + s);
                return null;
            };

            topicMap.put(topic, new Topic(q, operation, topic));
        }

        topicMap.get(topic).offer(payload);
        return newTopic;
    }
}
