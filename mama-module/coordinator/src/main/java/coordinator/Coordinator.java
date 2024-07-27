package com.huberlin.coordinator;

import com.huberlin.event.ControlEvent;
import java.io.*;
import java.net.*;

public class Coordinator {
  public static void main(String[] args) {
    int port = 6668;

    try (ServerSocket serverSocket = new ServerSocket(port)) {
      System.out.println("Coordinator started. Listening for connections on port " + port + "...");
      while (true) {
        Socket socket = serverSocket.accept();
        new ClientHandler(socket).start();
      }
    } catch (IOException e) {
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
            System.out.println("Reached the end of the stream for " + client_address + "\n");
            if (message == null)
              System.out.println("Stream terminated without end-of-the-stream marker.");
            input.close();
            socket.close();
            return;
          } else if (message.startsWith("control")) {
            ControlEvent controlEvent = ControlEvent.parse(message);
            assert controlEvent != null : "Parsing control event " + message + "failed";
            System.out.println("Received control event: " + controlEvent.toString());
          }
        }
      } catch (IOException e) {
        System.out.println("Client disconnected");
        try {
          socket.close();
        } catch (IOException ignored) {
        }
      }
    }
  }
}
