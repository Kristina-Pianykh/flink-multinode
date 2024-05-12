package com.huberlin.util.testing;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

/**
 * Localhost-to-localhost TCP connection that transmits test data
 *
 */
public class LoopbackTCPConnection {

    private final int rx_port;
    private final ServerSocketChannel rx_ssc;
    private SocketChannel rx_socket = null;
    private final Socket tx_socket = new Socket();
    public LoopbackTCPConnection() throws IOException, InterruptedException {
        //create serversocket
        rx_ssc = ServerSocketChannel.open();
        rx_ssc.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        this.rx_port = ((InetSocketAddress)rx_ssc.getLocalAddress()).getPort();
        rx_ssc.configureBlocking(true);
        //start listening
        Thread accepting_thread = new Thread(() -> rx_socket = accept(rx_port));
        accepting_thread.start();
        try {
            accepting_thread.join(1000); //FIXME: Reliably wait for ssc to be ready to accept connections (this is still a race condition! it can take more than 1s for the other thread to get scheduled)
        } catch (InterruptedException ignored) {}
        //Server now listening, establish connection...
        connect(rx_port);
        accepting_thread.join();
        //rx_socket now valid (TODO: make accepting thread a future, so it's clearer)
        //start data writing after a short delay
    }

    /**
     *
     * @return the connected, receiving SocketChanel. It's set to non-blocking mode by default.
     */
    public SocketChannel getRxSocketChannel(){
        return this.rx_socket;
    }

    private void connect(int rx_port) throws InterruptedException {
        while (true) {
            try {
                tx_socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), rx_port));
                return;
            } catch (IOException e) {
                Thread.sleep(100);
            }
        }
    }

    /**
     * Send data through the connection.
     *
     * @param data_to_transmit Data to send to the receiving socket via the connection
     * @param start_after_ms Delay before transmission begins
     * @param close_afterwards Whether to close the thread afterwards
     * @return the thread doing the transmission
     * @throws IOException
     */
    public Thread transmit_data(byte[] data_to_transmit, long start_after_ms, boolean close_afterwards) throws IOException {
        Thread tx_thread = new Thread(() -> {
            try {
                Thread.sleep(start_after_ms);
                tx_socket.getOutputStream().write(data_to_transmit);
                tx_socket.getOutputStream().flush();
                if (close_afterwards)
                    tx_socket.close();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        tx_thread.start();
        return tx_thread;
    }

    public Thread transmit_data(byte[] data_to_transmit) throws IOException {
        return transmit_data(data_to_transmit, 0, false);
    }

    SocketChannel accept(int rx_port){
        try {
            SocketChannel rx_socket = this.rx_ssc.accept();
                rx_socket.configureBlocking(false);
                return rx_socket;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
    }
}
