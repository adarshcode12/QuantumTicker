package com.trading.websocket;

import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.socket.TextMessage;

import java.util.Random;

public class RandomNumberWebSocketHandler extends TextWebSocketHandler {

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        new Thread(() -> {
            try {
                while (true) {
                    int randomValue = new Random().nextInt(100); // Generate a random number between 0 and 100
                    session.sendMessage(new TextMessage("Random number: " + randomValue));
                    Thread.sleep(1000); // Delay of 2 seconds
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }
}
