package com.mutliqueue;

public interface TopicsQueueAPI {

    boolean publish(String topic, String payload);

    TopicsQueue.Topic getTopicQueue(String poll);

    void removeTopic(String topic);
}
