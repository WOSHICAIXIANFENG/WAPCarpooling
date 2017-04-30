package com.carpool.util;

import java.net.InetSocketAddress;
import java.util.Collection;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

/**
 * Socket server --- send notification to clients
 * @author Xianfeng Cai
 */
public class SocketServer extends WebSocketServer {
    public static final int SOCKETPORT = 8889;
    public static volatile SocketServer instance = null;
    
    private SocketServer(int port) {
        super(new InetSocketAddress(port));
    }

    private SocketServer(InetSocketAddress address) {
        super(address);
    }
    
    public static SocketServer getInstance() {
    	if (instance == null) {
    		synchronized (SocketServer.class) {
    			if(instance == null ) {
    				instance = new SocketServer(SOCKETPORT);
    			}
    		}
    	}
    	
    	return instance;
    }

    // each client connect to my server, onOpen will be called
    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        try {
            System.out.println("client url: " + conn.getRemoteSocketAddress().getAddress().getHostAddress() + ":" + conn.getRemoteSocketAddress().getPort());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    
    /**
     * receive data from client host
     */
    @Override
    public void onMessage(WebSocket conn, String message) {
    	System.out.println("SoecketServer onMessage = " + message);
    	// default behavior
        broadcast(message);
    }

    // each client lost its connection, onClose will be called
    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
    	// 
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
    	
    }

    /**
     * Notify all clients. Client need to check if this msg come from itself
     * @param msg
     */
    private void broadcast(String msg) {
    	Collection<WebSocket> clients = connections();
        synchronized (clients) {
            for (WebSocket client : clients) {
                client.send(msg);
            }
        }
    }
   
    /**
     * function called by XxxxService to broadCast its data
     * 
     * @param msg
     */
    public void broadCastData(String jsonStr) {
    	Collection<WebSocket> clients = connections();
    	//System.out.println("Clients.size = " + clients.size());
    	broadcast(jsonStr);
    }
    
    /**
     * Check if there is any client is still connecting this server
     * @return boolean
     */
    public boolean hasAnyClientConnection() {
    	return !connections().isEmpty();
    }
}
