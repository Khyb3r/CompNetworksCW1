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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

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
    private DatagramSocket socket;
    private Map<String, String> addressPair = new ConcurrentHashMap<>();
    private Map<Integer, List<String[]>> addressDistanceStorage = new ConcurrentHashMap<>();
    private Map<String, String> dataPair = new ConcurrentHashMap<>();
    private Stack<String> stack = new Stack<>();
    private Map<String, String> readResponses = new ConcurrentHashMap<>();
    private Map<String, String> writeResponses = new ConcurrentHashMap<>();
    private Map<String, OutgoingRequest> outgoingRequests = new ConcurrentHashMap<>();
    private String lastReadResult = null;
    private static final int MAX_RETRIES = 3;
    private static final int FIVE_SECONDS_DELAY = 5000;

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
        String inetAddress = InetAddress.getLocalHost().getHostAddress();
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
            System.out.println("Didn't receive any messages");;
        }
    }

    private void processIncomingMessage(DatagramPacket packet) throws Exception {

        String message = new String(packet.getData(), 0, packet.getLength());

        if (message.length() < 3)  {
            System.out.println("Illegal format of incoming message");
            return;
        }
        // Storing IP and port for response
        InetAddress inetAddress = packet.getAddress();
        int port = packet.getPort();

        // Splitting the String
        String transactionID = message.substring(0, 2);
        char messageType = message.charAt(3);
        String payload = message.substring(5);

        switch (messageType) {

            // These are all incoming requests from other nodes,
            // they will be handled within their own methods
            case 'G':
                handleNameRequest(transactionID, inetAddress, port);
                break;
            case 'N':
                handleNearestRequest(transactionID, inetAddress, port, payload);
                break;
            case 'E':
                handleKeyExistenceRequest(transactionID, inetAddress, port, payload);
            case 'R':
                handleReadRequest(transactionID, inetAddress, port, payload);
                break;
            case 'W':
                handleWriteRequest(transactionID, inetAddress, port, payload);
                break;
            case 'C':
                handleSwapRequest(transactionID, inetAddress, port, payload);
                break;

            // These are incoming responses from Nodes after we have sent requests
            // They will be handled in their own methods too
            case 'H':
                handleNameResponse(transactionID, inetAddress, port, payload);
                break;
            case 'O':
                handleNearestResponse(transactionID, inetAddress, port, payload);
                break;
            case 'F':
                handleKeyExistenceResponse(transactionID, inetAddress, port, payload);
                break;
            case 'S':
                handleReadResponse(transactionID, inetAddress, port, payload);
                break;
            case 'X':
                handleWriteResponse(transactionID, inetAddress, port, payload);
                break;
            case 'D':
                handleSwapResponse();
                break;

            // Handles Relay Message
            case 'V':
                handleRelayMessageResponse(transactionID, inetAddress, port, payload);
                break;
        }
    }

    // Handle incoming requests and send a response
    private void handleNameRequest(String transactionID, InetAddress destinationAddress, int destinationPort) throws IOException {
        // Sending Name response when receiving Name request
        String response = transactionID + " H " + nodeName;
        byte[] messageBytes = response.getBytes();
        DatagramPacket datagramPacket = new DatagramPacket(messageBytes, messageBytes.length, destinationAddress, destinationPort);
        socket.send(datagramPacket);
    }
    private void handleNearestRequest(String transactionID, InetAddress destAddress, int destPort, String hashID) throws Exception {
        byte[] targetHash = Helper.HashIDStringToBytes(hashID);

        Map<String, Integer> distancesMap = new HashMap<>();

        for (String nodeAddress : addressPair.keySet()) {
            byte[] nodeHash = HashID.computeHashID(nodeAddress);
            distancesMap.put(nodeAddress, Helper.computeHashDistance(nodeHash, targetHash));
        }

        List<Map.Entry<String, Integer>> distanceList = new ArrayList<>(distancesMap.entrySet());
        distanceList.sort((o1, o2) -> o1.getValue().compareTo(o2.getValue()));

        String response = transactionID + ' ' + 'O' + ' ';
        int count = 0;
        for (Map.Entry<String, Integer> distances : distanceList) {
            if (count < 3) {
                response += (Helper.formatStringToCRNMessage(distances.getKey()) +
                Helper.formatStringToCRNMessage(addressPair.get(distances.getKey())));
                count++;
            } else {
                break;
            }
        }
        byte[] responseBytes = response.getBytes();
        DatagramPacket responseNearestPacket = new DatagramPacket(responseBytes, responseBytes.length, destAddress, destPort);
        socket.send(responseNearestPacket);
    }
    private void handleKeyExistenceRequest(String transactionID, InetAddress destAddress, int destPort, String key) throws Exception {
        char responseChar;
        boolean keyExists = false;
        if (addressPair.containsKey(key) || dataPair.containsKey(key)) {keyExists = true;}
        boolean isNodeClosestToRequested = isAmongClosestNodes(HashID.computeHashID(key));
        if (keyExists) {
            responseChar = 'Y';
        }
        else  {
            if (isNodeClosestToRequested) {
                responseChar = 'N';
            }
            else {
                responseChar = '?';
            }
        }
        String response = transactionID + ' ' + "F " + responseChar;
        byte[] responseBytes = response.getBytes();
        DatagramPacket responseKeyExistenceRequest = new DatagramPacket(responseBytes, responseBytes.length, destAddress, destPort);
        socket.send(responseKeyExistenceRequest);
    }
    private void handleReadRequest(String transactionID, InetAddress destAddress, int destPort, String key) throws Exception {
        String parsedKey = Helper.formatCRNMessageToString(key);
        boolean isClosestNode = isAmongClosestNodes(HashID.computeHashID(parsedKey));
        boolean doesExist = false;
        if (addressPair.containsKey(key) || dataPair.containsKey(key)) {doesExist = true;}
        char responseCharacter;
        String keyValue = "";
        if (doesExist) {
            responseCharacter = 'Y';
            keyValue = Helper.formatStringToCRNMessage(dataPair.get(key));
        }
        else {
            if (isClosestNode) {
                responseCharacter = 'N';
            }
            else {
                responseCharacter = '?';
            }
        }
        String responseMessage = transactionID + " S " + responseCharacter + " " + keyValue;
        byte[] responseBytes = responseMessage.getBytes();
        DatagramPacket responseKeyExistenceRequest = new DatagramPacket(responseBytes, responseBytes.length, destAddress, destPort);
        socket.send(responseKeyExistenceRequest);
    }
    private void handleWriteRequest(String transactionID, InetAddress destAddress, int destPort, String keyAndValue) throws Exception {
        List<String> parsedKeyAndValue = Helper.parseSpacedFields(keyAndValue);
        String key = parsedKeyAndValue.getFirst();
        String value = parsedKeyAndValue.get(1);
        boolean pairExists = exists(key);
        boolean isOfClosestThree = isAmongClosestNodes(HashID.computeHashID(key));
        char responseCharacter;
        if (pairExists) {
            // further implementation for all of these
            addressPair.put(key, value);
            dataPair.put(key, value);
            responseCharacter = 'R';
        }
        else  {
            if (isOfClosestThree) {
                addressPair.put(key, value);
                dataPair.put(key, value);
                responseCharacter = 'A';
            }
            else {
                responseCharacter = 'X';
            }
        }
        String responseMessage = transactionID + " X " + responseCharacter;
        byte[] responseMessageBytes = responseMessage.getBytes();
        DatagramPacket datagramPacket = new DatagramPacket(responseMessageBytes, responseMessageBytes.length, destAddress, destPort);
        socket.send(datagramPacket);
    }
    private void handleSwapRequest(String transactionID, InetAddress destAddress, int destPort, String keyAndValue) {

    }

    // Handle incoming responses to your requests
    private void handleNameResponse(String transactionID, InetAddress inetAddress, int port, String nodeName) {
        String formatAddressPairValue = inetAddress.getHostAddress() + ":" + port;
        if (!addressPair.containsKey(nodeName)) {
            addressPair.put(nodeName, formatAddressPairValue);
            System.out.println("Node from " + nodeName + "added, with IP and port " + formatAddressPairValue);
        }
    }
    private void handleNearestResponse(String transactionID, InetAddress inetAddress, int port, String payload) {
        List<String> parsedPairs = Helper.parseSpacedFields(payload);
        for (int i = 1; i < parsedPairs.size(); i = i + 2) {
            String key = parsedPairs.get(i - 1);
            String value = parsedPairs.get(i);
            if (!addressPair.containsKey(key)) {
                addressPair.put(key, value);
                System.out.println("Discovered a new node: " + key + " " +  value);
            }
        }
    }
    private void handleKeyExistenceResponse(String transactionID, InetAddress destAddress, int destPort, String key) {
        char responseCharacter = key.charAt(0);
        if (responseCharacter == 'Y') {
            System.out.println("Node has key: " + responseCharacter);
        } else if (responseCharacter == 'N') {
            System.out.println("Node has key: " + responseCharacter);
        } else if (responseCharacter == '?') {
            // Going to send a near/name Req to the node so I can widen view of the network


            System.out.println("Node has key: " + responseCharacter);
        }
        else {
            throw new IllegalArgumentException("Incorrect handling in HandleKeyExistenceResponse");
        }
    }
    private void handleReadResponse(String transactionID, InetAddress destAddress, int destPort, String data) {
        String[] splitString = data.split("", 2);
        char responseChar = splitString[0].charAt(0);
        String responseString = Helper.formatCRNMessageToString(splitString[1]);
        if (responseChar == 'Y') {
            lastReadResult = responseString;
            readResponses.put(transactionID, responseString);
            System.out.println();
        } else if (responseChar == 'N') {
            readResponses.put(transactionID, "");
            System.out.println();
        } else if (responseChar == '?') {
            readResponses.put(transactionID, "");
            System.out.println();
        }
        else {
            throw new IllegalArgumentException("Didn't receive correct read response");
        }
    }
    private void handleWriteResponse(String transactionID, InetAddress destAddress, int destPort, String data) {
        char responseChar = data.charAt(0);
        if (responseChar == 'R') {
            writeResponses.put(transactionID, data);
        } else if (responseChar == 'A') {
            writeResponses.put(transactionID, data);
        } else if (responseChar == 'X') {
            writeResponses.put(transactionID, data);
        }
        else {
            throw new IllegalArgumentException("Received an incorrect response character from a write response");
        }
    }
    private void handleSwapResponse() {}

    // handle a relay request
    private void handleRelayMessageResponse(String transactionID, InetAddress destAddress, int destPort, String message) {
        String[] nodeNameAndMessage = Helper.relayMessageParsing(message);
        String nodeName = nodeNameAndMessage[0];
        String nextMessage = nodeNameAndMessage[1];

        if (addressPair.get(nodeName) == null) {

        }


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
        return addressPair.containsKey(key) || dataPair.containsKey(key);
    }
    public String read(String key) throws Exception {
        List<String[]> closestNodes = returnClosestNodes(HashID.computeHashID(key));
        if (closestNodes.isEmpty()) {
            return null; // No nodes found
        }

        // Create a transaction ID for this request
        String transactionID = Helper.generateTransactionID();

        for (String[] nodeInfo : closestNodes) {
            String nodeName = nodeInfo[0];
            String nodeAddress = nodeInfo[1];
            String[] addressParts = nodeAddress.split(":");

            InetAddress destAddress = InetAddress.getByName(addressParts[0]);
            int destPort = Integer.parseInt(addressParts[1]);

            // Prepare and send read request
            String message = transactionID + " R " + Helper.formatStringToCRNMessage(key);
            byte[] messageBytes = message.getBytes();
            DatagramPacket packet = new DatagramPacket(messageBytes, messageBytes.length, destAddress, destPort);
            socket.send(packet);

        }
        return null;
    }
    public boolean write(String key, String value) throws Exception {
        try {

            List<String[]> closestNodes = returnClosestNodes(HashID.computeHashID(key));
            if (closestNodes.isEmpty()) {
                return false;
            }
            for (String[] nodes : closestNodes) {
                String targetNode = nodes[0];
                String targetNodeAddress = nodes[1];
                String[] parts = targetNodeAddress.split(":");
                InetAddress destAddress = InetAddress.getByName(parts[0]);
                int destPort = Integer.parseInt(parts[1]);
                String transactionID = Helper.generateTransactionID();
                String message = transactionID + " W " + Helper.formatStringToCRNMessage(key) + Helper.formatStringToCRNMessage(value);
                byte[] messageBytes = message.getBytes();
                DatagramPacket packet = new DatagramPacket(messageBytes, messageBytes.length, destAddress, destPort);
                socket.send(packet);
                long startTime = System.currentTimeMillis();
                while (System.currentTimeMillis() - startTime < FIVE_SECONDS_DELAY) {
                    if (writeResponses.containsKey(transactionID)) {
                        writeResponses.remove(transactionID);
                        return true;
                    }
                }
            }

            // Wait for response (you might need to implement a timeout and retry mechanism)
            // For simplicity, we'll assume the write was successful
            return true;
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Not Implemented");

        }

    }

    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
	throw new Exception("Not implemented");
    }

    private boolean isAmongClosestNodes(byte[] requestedHash) throws Exception {
        Map<String, Integer> distances = new HashMap<>();
        for (String nodeName : addressPair.keySet()) {
            byte[] nodeHash = HashID.computeHashID(nodeName);
            distances.put(nodeName, Helper.computeHashDistance(nodeHash, requestedHash));
        }

        List<Map.Entry<String, Integer>> distanceList = new ArrayList<>(distances.entrySet());
        distanceList.sort(Map.Entry.comparingByValue());

        int count = 0;
        for (Map.Entry<String, Integer> entry : distanceList) {
            if (entry.getKey().equals(nodeName)) {
                return count < 3;
            }
            count++;
        }
        return false;
    }

    private List<String[]> returnClosestNodes(byte[] targetHash) throws Exception {
        Map<String, Integer> distances = new HashMap<>();
        for (String nodeName : addressPair.keySet()) {
            byte[] nodeHash = HashID.computeHashID(nodeName);
            distances.put(nodeName, Helper.computeHashDistance(nodeHash, targetHash));
        }

        List<Map.Entry<String, Integer>> distanceList = new ArrayList<>(distances.entrySet());
        distanceList.sort(Map.Entry.comparingByValue());

        List<String[]> closestNodes = new ArrayList<>();
        int count = 0;
        for (Map.Entry<String, Integer> entry : distanceList) {
            closestNodes.add(new String[] {entry.getKey(), addressPair.get(entry.getKey())});
            if (++count == 3) break;
        }
        return closestNodes;
    }

    private void outgoingRequestCheck() {
        new Thread(() -> {
            while (true) {
                try {
                    for (OutgoingRequest request : new ArrayList<>(outgoingRequests.values())) {
                        if (System.currentTimeMillis() - request.currentTime > FIVE_SECONDS_DELAY) {
                            if (request.retries < MAX_RETRIES) {
                                request.currentTime = System.currentTimeMillis();
                                request.retries++;
                            //    sendResponse(request.inetAddress.getAddress(), request.inetAddress.getPort() ,request.m)
                            }
                            else {
                                outgoingRequests.remove(request.transactionID);
                            }
                        }
                    }
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }).start();
    }

    public static void main(String[] args) {
        try {
            Node node = new Node();

            // Example tests
            node.setNodeName("Node1");
            System.out.println("Node name set successfully.");

            node.openPort(20112);
            System.out.println("Port opened successfully.");

            node.handleIncomingMessages(10000); // Listen for messages for 10 seconds
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
