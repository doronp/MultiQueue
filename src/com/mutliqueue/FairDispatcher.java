package com.mutliqueue;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FairDispatcher {

    public static final int BULK_SIZE = 3;
    private TopicsQueueAPI topicQueue;
    private static int MAX_TOPICS = 1000;
    ExecutorService executor = null;
    ArrayBlockingQueue<String> topics = null;

    private static final Logger logger = Logger.getLogger(FairDispatcher.class.getName());


    FairDispatcher(TopicsQueueAPI topicQueue) {
        this.topicQueue = topicQueue;
    }


    /**
     * Starting a dispatcher with newWorkStealingPool which is thread pool with parallelism as the number of cores.
     */
    public void start() {
        logger.entering(getClass().getName(), "start()");
        topics = new ArrayBlockingQueue<>(MAX_TOPICS, true);

        /**
         * Each worker will choose the next topic, handle at most BULK_SIZE messages from it -
         * and return it to the end of the topics queue. This maintains fairness and prevents topic starvation.
         */
        Runnable Worker = () -> {
            while (true) {
                try {
                    String topic = topics.take();
                    TopicsQueue.Topic t = topicQueue.getTopicQueue(topic);
                    if (t != null) {
                        ArrayList<String> elements = new ArrayList<>(BULK_SIZE);
                        t.messages.drainTo(elements);
                        for (String elm : elements) {
                            t.operation.apply(elm);
                        }
                        topics.put(topic);
                    } else {
                        topicQueue.removeTopic(topic);
                    }
                } catch (InterruptedException e) {
                    logger.log(Level.SEVERE, "Error while working on topics...", e);
                }
            }
        };

        executor = Executors.newWorkStealingPool();
        for (int i = 0; i < Runtime.getRuntime().availableProcessors(); ++i) {
            executor.submit(Worker);
        }
    }

    /**
     * @param topic - Once a never seen before topic is seen it is added to the list of topics.
     *              The workers 'round robin' the list of topics.
     */
    public void notifyOnNewTopic(String topic) {
        try {
            topics.put(topic);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Error while adding new topic", e);
        }
    }

}


