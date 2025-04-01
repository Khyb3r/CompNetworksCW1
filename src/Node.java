// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  YOUR_NAME_GOES_HERE
//  YOUR_STUDENT_ID_NUMBER_GOES_HERE
//  YOUR_EMAIL_GOES_HERE


// DO NOT EDIT starts
// This gives the interface that your code must implement.
// These descriptions are intended to help you understand how the interface
// will be used. See the RFC for how the protocol works.

import java.io.IOException;
import java.net.*;
import java.util.*;

interface NodeInterface {

    /* These methods configure your node.
     * They must both be called once after the node has been created but
     * before it is used. */
    
    // Set the name of the node.
    public void setNodeName(String nodeName) throws Exception;

    // Open a UDP port for sending and receiving messages.
    public void openPort(int portNumber) throws Exception;


    /*
     * These methods query and change how the network is used.
     */

    // Handle all incoming messages.
    // If you wait for more than delay miliseconds and
    // there are no new incoming messages return.
    // If delay is zero then wait for an unlimited amount of time.
    public void handleIncomingMessages(int delay) throws Exception;
    
    // Determines if a node can be contacted and is responding correctly.
    // Handles any messages that have arrived.
    public boolean isActive(String nodeName) throws Exception;

    // You need to keep a stack of nodes that are used to relay messages.
    // The base of the stack is the first node to be used as a relay.
    // The first node must relay to the second node and so on.
    
    // Adds a node name to a stack of nodes used to relay all future messages.
    public void pushRelay(String nodeName) throws Exception;

    // Pops the top entry from the stack of nodes used for relaying.
    // No effect if the stack is empty
    public void popRelay() throws Exception;
    

    /*
     * These methods provide access to the basic functionality of
     * CRN-25 network.
     */

    // Checks if there is an entry in the network with the given key.
    // Handles any messages that have arrived.
    public boolean exists(String key) throws Exception;
    
    // Reads the entry stored in the network for key.
    // If there is a value, return it.
    // If there isn't a value, return null.
    // Handles any messages that have arrived.
    public String read(String key) throws Exception;

    // Sets key to be value.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean write(String key, String value) throws Exception;

    // If key is set to currentValue change it to newValue.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean CAS(String key, String currentValue, String newValue) throws Exception;

}
// DO NOT EDIT ends

// Complete this!
public class Node implements NodeInterface {
    private String nodeName;
    private int portNumber;
    private DatagramSocket socket;
    private Map<String, String> addressPair = new HashMap<>();
    private Map<String, String> dataPair = new HashMap<>();
    private Stack<String> stack = new Stack<>();

 /*   private String[] formatAddressPair(String key, String value) {
        String[] pair = new String[2];
        key = nodeName;
        value =
    } */


    public void setNodeName(String nodeName) throws Exception {
        if (nodeName.isEmpty()) {
            throw new Exception("Node name is empty");
        }
        this.nodeName = nodeName;

    }
    private String[] formatAddressPair(String nodeName, String inetAddress, int portNumber) {
        String[] pair = new String[2];
        pair[0] = "N:" + nodeName;
        pair[1] = inetAddress + ":" + portNumber;
        return pair;
    }
    public void openPort(int portNumber) throws Exception {
        if (portNumber >= 20110 && portNumber <= 20130) {
            try {
                this.portNumber = portNumber;
                this.socket = new DatagramSocket(portNumber);
                System.out.println("Socket at port: " + portNumber + " is ready to recieve connections");
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        else {
            throw new Exception("Invalid port number keep in range between 20110 and 20130");
        }
      //  String addressKey = "N:" + nodeName;
        String inetAddress = socket.getLocalAddress().getHostAddress();
        String[] pairForAddresses = formatAddressPair(nodeName, inetAddress, portNumber);
        addressPair.put(pairForAddresses[0], pairForAddresses[1]);
        System.out.println(pairForAddresses[0] + " " + pairForAddresses[1]);
    }

    public void handleIncomingMessages(int delay) throws Exception {
        if (socket == null) {
            throw new IllegalStateException("Port should be open before handling messages");
        }
        socket.setSoTimeout(delay > 0 ? delay : 0);

        byte[] buffer = new byte[1024];
        DatagramPacket datagramPacket = new DatagramPacket(buffer, buffer.length);
        try {
            socket.receive(datagramPacket);
            processIncomingMessage(datagramPacket);
        }
        catch (SocketTimeoutException e) {
            System.out.println("Didn't recieve any messages");;
        }
    }
    private void processIncomingMessage(DatagramPacket packet) throws Exception {

        String message = new String(packet.getData(), 0, packet.getLength());
        if (message.length() < 4) return;

        // Storing IP and port for response
        InetAddress inetAddress = packet.getAddress();
        int port = packet.getPort();
        String transactionID = message.substring(0, 2);

        String payload = message.substring(3);
        char messageType = payload.charAt(0);
        System.out.println(transactionID);

        System.out.println(transactionID + " " + payload + " " + messageType);
        switch (messageType) {
            case 'G':
                sendNameResponse(transactionID, inetAddress, port);
                break;
            case 'H':
                sendNearestResponse(transactionID, inetAddress, port, payload.substring(2));
                break;
            case 'N':
                break;
            case 'O':
                break;
            case 'I':
                break;
            case 'V':
                break;
        }
    }
    private void sendNameResponse(String transactionID, InetAddress destinationAddress, int destinationPort) throws IOException {
        // Sending Name response when receiving Name request
        String response = transactionID + " " + 'H' + ' ' + nodeName;
        byte[] messageBytes = response.getBytes();
        DatagramPacket datagramPacket = new DatagramPacket(messageBytes, messageBytes.length, destinationAddress, destinationPort);
        socket.send(datagramPacket);
    }

    private void sendNearestResponse(String transactionID, InetAddress destAddress, int port, String hashID) throws Exception {
        byte[] targetHash = new byte[32];
        for (int i = 0; i < 32; i++) {
            targetHash[i] = (byte) Integer.parseInt(hashID.substring(i * 2, i * 2 + 2), 16);
        }

        Map<String, Integer> distances = new HashMap<>();

        for (String nodeAddr : addressPair.keySet()) {
            byte[] nodeHash = Helper.getHashID(nodeAddr);
            distances.put(nodeAddr, Helper.computeHashDistance(nodeHash, targetHash));
        }

        List<Map.Entry<String, Integer>> distanceList = new ArrayList<>(distances.entrySet());
        Collections.sort(distanceList, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });

        String response = transactionID + ' ' + "O ";
        int count = 0;
        for (Map.Entry<String, Integer> entry : distanceList) {
            if (count < 3) {
                response += formatString(entry.getKey() + " " + addressPair.get(entry.getKey()));
                count++;
            } else {
                break;
            }
        }
        byte[] responseBytes = response.getBytes();
        DatagramPacket responsePacket = new DatagramPacket(responseBytes, responseBytes.length, destAddress, port);
        socket.send(responsePacket);
    }
    private String formatString(String str) {
        return str.split(" ").length - 1 + " " + str + " ";
    }

    public boolean isActive(String nodeName) throws Exception {
        for (String addressNodeName : addressPair.keySet()) {

        }
        String activeMessage = "";
	 //   DatagramPacket datagramPacket = new DatagramPacket();
        throw new Exception("Not implemented");
    }
    
    public void pushRelay(String nodeName) throws Exception {
	    stack.push(nodeName);
       // throw new Exception("Not implemented");
    }

    public void popRelay() throws Exception {
        if (!stack.isEmpty()) {
            stack.pop();
        }
     //   throw new Exception("Not implemented");
    }

    public boolean exists(String key) throws Exception {
	throw new Exception("Not implemented");
    }
    
    public String read(String key) throws Exception {
	throw new Exception("Not implemented");
    }

    public boolean write(String key, String value) throws Exception {
	throw new Exception("Not implemented");
    }

    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
	throw new Exception("Not implemented");
    }


    public static void main(String[] args) {
        try {
            Node node = new Node();

            // Example tests
            node.setNodeName("Node1");
            System.out.println("Node name set successfully.");

            node.openPort(20112);
            System.out.println("Port opened successfully.");

            node.handleIncomingMessages(1000); // Listen for messages for 10 seconds
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
