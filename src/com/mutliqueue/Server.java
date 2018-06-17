package com.mutliqueue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server {

    private static final String KILL_MESSAGE = "KILL_MESSAGE";
    private static final Logger logger = Logger.getLogger(Server.class.getName());
    public static final int PORT = 5454;
    public static final int BUFFER_SIZE = 256;

    private FairDispatcher dispatcher;
    private TopicsQueue topicsQueue;

    public static void main(String[] args) throws IOException {
        logger.log(Level.INFO, "Starting server.. ");
        new Server().startServer();
    }

    /**
     * Start the server. Using NIO one thread serves multiple connections.
     * (As opposed to use a thread for each connection).
     * <p>
     * User == Topic in our case BTW.
     * <p>
     * Some boilerplate code for NIO communication.
     *
     * @throws IOException
     */

    private void startServer() throws IOException {
        Selector selector = Selector.open();
        ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.bind(new InetSocketAddress("localhost", PORT));
        serverSocket.configureBlocking(false);
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

        topicsQueue = new TopicsQueue();
        dispatcher = new FairDispatcher(topicsQueue);
        dispatcher.start();


        while (true) {
            int readyCount = selector.select();
            if (readyCount == 0) continue;
            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            Iterator<SelectionKey> iterator = selectedKeys.iterator();
            while (iterator.hasNext() && readyCount > 0) {
                SelectionKey key = iterator.next();

                if (key.isAcceptable()) {
                    register(selector, serverSocket);
                }

                if (key.isReadable()) {
                    handleMessage(buffer, key);

                }
                iterator.remove();
            }
        }
    }


    private void handleMessage(ByteBuffer buffer, SelectionKey key) throws IOException {

        SocketChannel channel = (SocketChannel) key.channel();
        channel.read(buffer);
        String msg = new String(buffer.array()).trim();
        logger.log(Level.INFO, msg);

        if (msg.equals(KILL_MESSAGE)) {
            logger.log(Level.INFO, "Not accepting client messages anymore");

            channel.close();
            return;
        }

        String[] umArr = msg.split(":");
        if (umArr.length == 2 && topicsQueue.publish(umArr[0], umArr[1])) {
            dispatcher.notifyOnNewTopic(umArr[0]);
        }

        buffer.flip();
        channel.write(buffer);
        buffer.clear();
    }


    private static void register(Selector selector, ServerSocketChannel serverSocket)
            throws IOException {

        SocketChannel client = serverSocket.accept();
        client.configureBlocking(false);
        client.register(selector, SelectionKey.OP_READ);
    }

    /*
    public static Process start() throws IOException, InterruptedException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = Server.class.getCanonicalName();

        ProcessBuilder builder = new ProcessBuilder(javaBin, "-cp", classpath, className);

        return builder.start();
    }
*/

}