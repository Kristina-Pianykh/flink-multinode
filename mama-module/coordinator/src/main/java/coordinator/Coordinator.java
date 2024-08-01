package com.huberlin.coordinator;

import com.huberlin.event.ControlEvent;
import java.io.*;
import java.net.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Coordinator {
  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);

  public static void main(String[] args) {
    int port = 6668;

    try (ServerSocket serverSocket = new ServerSocket(port)) {
      LOG.info("Coordinator started. Listening for connections on port " + port + "...");
      while (true) {
        Socket socket = serverSocket.accept();
        new ClientHandler(socket).start();
      }
    } catch (IOException e) {
      LOG.error("Failed to start coordinator with error: {}", e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }
  }

  private static class ClientHandler extends Thread {
    private Socket socket;

    public ClientHandler(Socket socket) {
      this.socket = socket;
    }

    @Override
    public void run() {
      try {
        BufferedReader input =
            new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
        String client_address = socket.getRemoteSocketAddress().toString();

        while (true) {
          String message = input.readLine();

          if (message == null || message.contains("end-of-the-stream")) {
            LOG.info("Reached the end of the stream for " + client_address + "\n");
            if (message == null) LOG.info("Stream terminated without end-of-the-stream marker.");
            input.close();
            socket.close();
            return;
          } else if (message.startsWith("control")) {
            Optional<ControlEvent> controlEvent = ControlEvent.parse(message);
            assert controlEvent.isPresent() : "Parsing control event " + message + "failed";
            LOG.info("Received control event: " + controlEvent.get().toString());
          }
        }
      } catch (IOException e) {
        LOG.error("Client disconnected with error {}", e.getMessage());
        try {
          socket.close();
        } catch (IOException exc) {
          LOG.error("Failed to close socket with error {}", exc.getMessage());
        }
      }
    }
  }
}
