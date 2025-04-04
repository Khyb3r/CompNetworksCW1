// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  Khyber Jan
//  230023103
//  Khyber.Jan@city.ac.uk


// DO NOT EDIT starts
// This gives the interface that your code must implement.
// These descriptions are intended to help you understand how the interface
// will be used. See the RFC for how the protocol works.

import java.net.*;
import java.util.*;
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
    private final ConcurrentHashMap<String, String> addressPair = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> dataPair = new ConcurrentHashMap<>();
    private final Stack<String> relayStack = new Stack<>();
    private final ConcurrentHashMap<Integer, List<String>> addressOfCalculatedDistances = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SentRequestHelperTracker> sentRequests = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> pendingResponses = new ConcurrentHashMap<>();
    private final HashSet<String> knownNodes = new HashSet<>();
    private final String generateTransactionID = Helper.generateTransactionID();
    private static final int FIVE_SECOND_DELAY = 5000;
    private static final int RETRIES = 3;

    @Override
    public void setNodeName(String nodeName) throws Exception {
        if (nodeName == null || nodeName.isEmpty()) {
            throw new Exception("Node name cannot be null or empty");
        }
        if (!nodeName.startsWith("N:")) {
            throw new Exception("Node name must start with 'N:'");
        }
        if (addressPair.containsKey(nodeName)) {
            throw new Exception("Node names must all be unique");
        }

        this.nodeName = nodeName;
        addressPair.put(nodeName, "");
        addDistance(0, nodeName);
    }

    @Override
    public void openPort(int portNumber) throws Exception {
        if (socket != null && !socket.isClosed()) {
            throw new Exception("Port already open");
        }
        this.socket = new DatagramSocket(portNumber);
        String localAddress = socket.getLocalAddress().getHostAddress() + ":" + portNumber;
        addressPair.put(nodeName, localAddress);
    }

    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        if (socket == null) {
            throw new Exception("Port not open");
        }

        socket.setSoTimeout(delay);
        byte[] buffer = new byte[1024];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

        long startTime = System.currentTimeMillis();
        while (delay == 0 || System.currentTimeMillis() - startTime < delay) {
            try {
                socket.receive(packet);
                String message = new String(packet.getData(), 0, packet.getLength());
                processMessage(message, packet.getAddress(), packet.getPort());
            } catch (SocketTimeoutException e) {
                break;
            } catch (Exception e) {
                System.err.println("Error handling message: " + e.getMessage());
            }
        }
    }

    private void processMessage(String message, InetAddress senderAddress, int port) throws Exception {
        List<String> parts = Helper.parseSpacedFields(message);
        if (parts.size() < 2) {
            throw new Exception("Invalid message format");
        }

        String transactionID = parts.get(0);
        String messageTypeChar = parts.get(1);

        switch (messageTypeChar) {
            // Handle Requests
            case "G":
                handleNameRequest(transactionID, senderAddress, port);
                break;
            case "N":
                handleNearestRequest(parts, senderAddress, port);
                break;
            case "E" :
                handleKeyExistenceRequest(parts, senderAddress, port);
                break;
            case "R":
                handleReadRequest(parts, senderAddress, port);
                break;
            case "W":
                handleWriteRequest(parts, senderAddress, port);
                break;
            case "C":
                handleCASRequest(parts, senderAddress, port);
                break;

            // Handle Responses
            case "H":
                handleNameResponse(parts, senderAddress, port);
                break;
            case "O":
                handleNearestResponse(parts);
                break;
            case "F":
                handleKeyExistsResponse(parts);
                break;
            case "S":
                handleReadResponse(parts);
                break;
            case "X":
                handleWriteResponse(parts);
                break;
            case "D":
                handleCASResponse(parts);
            // Info Response
            case "I":
                handleInfoMessageRequest(parts);
                break;
            // Relay message handler
            case "V":
                handleRelayMessage(message);
            default: throw new Exception("Unknown message type: " + messageTypeChar);
        }
    }


    // Handling requests methods
    private void handleNameRequest(String transactionID, InetAddress address, int port) throws Exception {
        String response = transactionID + " H " + Helper.formatSpaces(nodeName) + nodeName + " ";
        sendResponse(response, address, port);
    }
    private void sendNearestRequest(String targetNode, InetAddress address, int port) throws Exception {
        String targetHash = Helper.fromBytesToHexFormat(HashID.computeHashID(targetNode));
        String transactionID = generateTransactionID;
        String request = transactionID + " N " + Helper.formatSpaces(targetHash) + targetHash + " ";
        sendRequest(request, address, port, "N");
        sentRequests.put(transactionID, new SentRequestHelperTracker(request.getBytes(), address, port, "N"));
    }
    private void handleKeyExistenceRequest(List<String> payload, InetAddress address, int port) throws Exception {

        String transactionID = payload.get(0);
        String requestTypeChar = payload.get(1);
        String key = payload.get(2);
        String responseMessage = "";
        boolean hasKey = exists(key);
        boolean isClosest = isKeyOfThreeClosest(key);

        switch (requestTypeChar) {
            case "E":  // Key Existence Request
                if (hasKey) {
                    responseMessage = transactionID + " F Y ";

                } else if (isClosest) {
                    responseMessage = transactionID + " F N ";
                } else {
                    responseMessage = transactionID + " F ? ";
                }
        }
        sendResponse(responseMessage, address, port);
    }
    private void handleNearestRequest(List<String> payload, InetAddress senderAddress, int port) throws Exception {
        if (payload.size() != 3) {
            throw new Exception("Invalid nearest request format");
        }

        String transactionID = payload.get(0);
        String targetHash = payload.get(2);

        // Find closest nodes to the target hash
        List<String> closestNodes = closestNodesAlgorithm(targetHash, 3);
        StringBuilder response = new StringBuilder(transactionID + " O ");

        for (String node : closestNodes) {
            String address = addressPair.get(node);
            if (address != null) {
                response.append(Helper.formatSpaces(node)).append(node).append(" ")
                        .append(Helper.formatSpaces(address)).append(address).append(" ");
            }
        }

        sendResponse(response.toString(), senderAddress, port);
    }
    private void handleReadRequest(List<String> payload, InetAddress address, int port) throws Exception {
        if (payload.size() != 3) {
            throw new Exception("Invalid read request format");
        }

        String transactionID = payload.get(0);
        String key = payload.get(2);
        String response;

        // Check if we should respond to this request
        boolean shouldRespond = shouldStoreLocally(key);

        if (shouldRespond) {
            if (key.startsWith("N:") && addressPair.containsKey(key)) {
                response = transactionID + " S Y " + Helper.formatSpaces(addressPair.get(key)) + addressPair.get(key) + " ";
            } else if (key.startsWith("D:") && dataPair.containsKey(key)) {
                response = transactionID + " S Y " + Helper.formatSpaces(dataPair.get(key)) + dataPair.get(key) + " ";
            } else {
                response = transactionID + " S N ";
            }
        } else {
            response = transactionID + " S ? ";
        }

        sendResponse(response, address, port);
    }
    private void handleWriteRequest(List<String> payload, InetAddress address, int port) throws Exception {
        if (payload.size() != 4) {
            throw new Exception("Invalid write request format");
        }

        String transactionID = payload.get(0);
        String key = payload.get(2);
        String value = payload.get(3);
        String responseChar;

        // Check if we're one of the closest nodes
        boolean shouldStore = shouldStoreLocally(key);

        if (key.startsWith("N:")) {
            if (shouldStore) {
                addressPair.put(key, value);
                responseChar = "A"; // Accepted new value
            } else {
                responseChar = "X"; // Not responsible
            }
        } else if (key.startsWith("D:")) {
            if (shouldStore) {
                if (dataPair.containsKey(key)) {
                    dataPair.put(key, value);
                    responseChar = "R"; // Replaced existing value
                } else {
                    dataPair.put(key, value);
                    responseChar = "A"; // Accepted new value
                }
            } else {
                responseChar = "X"; // Not responsible
            }
        } else {
            responseChar = "X"; // Invalid key
        }

        String response = transactionID + " X " + responseChar + " ";
        sendResponse(response, address, port);
    }
    private void handleCASRequest(List<String> payload, InetAddress address, int port) throws Exception {
        if (payload.size() != 5) {
            throw new Exception("Invalid CAS request format");
        }

        String transactionID = payload.get(0);
        String key = payload.get(2);
        String requestedValue = payload.get(3);
        String newValue = payload.get(4);
        String responseChar = "X";


        if (shouldStoreLocally(key)) {
                if (key.startsWith("D:")) {
                    if (dataPair.containsKey(key) && dataPair.get(key).equals(requestedValue)) {
                        dataPair.put(key, newValue);
                        responseChar = "R";
                    } else if (!dataPair.containsKey(key)) {
                        dataPair.put(key, newValue);
                        responseChar = "A";
                    } else {
                        responseChar = "N";
                    }
                } else if (key.startsWith("N:")) {
                    if (addressPair.containsKey(key) && addressPair.get(key).equals(requestedValue)) {
                        addressPair.put(key, newValue);
                        responseChar = "R";
                    } else if (!addressPair.containsKey(key)) {
                        addressPair.put(key, newValue);
                        responseChar = "A";
                    } else {
                        responseChar = "N"; //
                    }
                }
        }

        String response = transactionID + " D " + responseChar + " ";
        sendResponse(response, address, port);
    }


    // Handling responses methods
    private void handleNameResponse(List<String> payload, InetAddress address, int port) throws Exception {
        if (payload.size() != 3) {
            throw new Exception("Invalid name response format");
        }

        String nodeName = payload.get(2);
        String nodeAddress = address.getHostAddress() + ":" + port;

        addressPair.put(nodeName, nodeAddress);
        knownNodes.add(nodeAddress);

        String nodeHash = Helper.fromBytesToHexFormat(HashID.computeHashID(nodeName));
        String ourHash = Helper.fromBytesToHexFormat(HashID.computeHashID(this.nodeName));
        int distance = Helper.computeHashDistance(nodeHash, ourHash);
        addDistance(distance, nodeName);

        sendNearestRequest(nodeName, address, port);
    }
    private void handleNearestResponse(List<String> payload) throws Exception {
        if (payload.size() < 4 || payload.size() % 2 != 0) {
            System.err.println("Invalid nearest response format");
            return;
        }

        for (int i = 2; i < payload.size(); i += 2) {
            String nodeName = payload.get(i);
            String nodeAddress = payload.get(i + 1);

            if (nodeName.startsWith("N:")) {
                addressPair.put(nodeName, nodeAddress);
                knownNodes.add(nodeAddress);

                // Update distance information
                String nodeHash = Helper.fromBytesToHexFormat(HashID.computeHashID(nodeName));
                String ourHash = Helper.fromBytesToHexFormat(HashID.computeHashID(this.nodeName));
                int distance = Helper.computeHashDistance(nodeHash, ourHash);
                addDistance(distance, nodeName);
            }
        }
    }
    private void handleKeyExistsResponse(List<String> payload) {
        if (payload.size() == 3 && payload.get(1).equals("F")) {
            pendingResponses.put("E:" + payload.get(2), "F");
        }
    }
    private void handleReadResponse(List<String> payload) {
        if (payload.size() >= 4 && payload.get(1).equals("Y")) {
            pendingResponses.put(payload.get(0), payload.get(3));
        }
    }
    private void handleWriteResponse(List<String> payload) {
        if (payload.size() == 3) {
            pendingResponses.put(payload.get(0), payload.get(2));
        }
    }
    private void handleCASResponse(List<String> payload) {
        if (payload.size() == 3 && payload.get(1).equals("D")) {
            pendingResponses.put(payload.get(0), payload.get(2));
        }
    }

    // Handling Information Request
    private void handleInfoMessageRequest(List<String> payload) {
        if (payload.size() >= 3 && payload.get(1).equals("I")) {
            System.out.println("Information message: " + String.join(" ", payload.subList(2, payload.size())));
        }
    }

    //Handling inbound relay message
    private void handleRelayMessage(String payload) throws Exception {
        String[] splitRelayMessage = payload.split(" ", 3);
        String transactionID = splitRelayMessage[0];
        String[] restOfMessage = Helper.relayMessageParsing(splitRelayMessage[2]);
        String nodeName = restOfMessage[0];
        String messageToBeForwarded = restOfMessage[1];

        pushRelay(nodeName);
        try {
            if (nodeName.equals(this.nodeName)) {
                return;
            }
            if (!addressPair.containsKey(nodeName)) {
                throw new Exception("Unknown relay target node: " + nodeName);
            }
            InetSocketAddress nextNodeAddress = Helper.returnSocketAddress(addressPair.get(nodeName));
            if (nextNodeAddress == null) {
                throw new Exception("Invalid address for relay target: " + nodeName);
            }
            String forwardMessage = transactionID + " V " + nodeName + " " + messageToBeForwarded;
            sendResponse(forwardMessage, nextNodeAddress.getAddress(), nextNodeAddress.getPort());
            sentRequests.put(transactionID, new SentRequestHelperTracker(forwardMessage.getBytes(),
                        nextNodeAddress.getAddress(),
                                nextNodeAddress.getPort(),
                                            "V"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            popRelay();
        }
    }

    private void sendResponse(String message, InetAddress destAddress, int destPort) throws Exception {
        byte[] responseData = message.getBytes();
        DatagramPacket packet = new DatagramPacket(responseData, responseData.length, destAddress, destPort);
        socket.send(packet);
    }

    private void sendRequest(String message, InetAddress destAddress, int destPort, String requestType) throws Exception {
        byte[] requestData = message.getBytes();
        DatagramPacket packet = new DatagramPacket(requestData, requestData.length, destAddress, destPort);
        SentRequestHelperTracker i = new SentRequestHelperTracker(requestData, destAddress, destPort, requestType);
        sentRequests.put(message.substring(0, 2), i);
        socket.send(packet);
    }

    @Override
    public boolean isActive(String nodeName) throws Exception {
        if (!addressPair.containsKey(nodeName)) {
            return false;
        }

        InetSocketAddress address = Helper.returnSocketAddress(addressPair.get(nodeName));
        if (address == null) {
            return false;
        }

        String transactionID = generateTransactionID;
        String request = transactionID + " G ";
        sendRequest(request, address.getAddress(), address.getPort(), "G");

        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 1000) {
            handleIncomingMessages(100);
            if (!sentRequests.containsKey(transactionID)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
    }

    @Override
    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            relayStack.pop();
        }
    }

    @Override
    public boolean exists(String key) throws Exception {
        if (key.startsWith("N:") && addressPair.containsKey(key)) {
            return true;
        }
        if (key.startsWith("D:") && dataPair.containsKey(key)) {
            return true;
        }

        List<String> closestNodes = closestNodesAlgorithm(key, 3);
        String transactionID = generateTransactionID;

        for (String node : closestNodes) {
            if (!node.equals(nodeName) && addressPair.containsKey(node)) {
                InetSocketAddress address = Helper.returnSocketAddress(addressPair.get(node));
                if (address != null) {
                    String request = transactionID + " E " + Helper.formatSpaces(key) + key + " ";
                    sendRequest(request, address.getAddress(), address.getPort(), "E");
                }
            }
        }

        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 1000) {
            handleIncomingMessages(100);
            if (pendingResponses.containsKey("E:" + key)) {
                return pendingResponses.get("E:" + key).equals("F");
            }
        }

        return false;
    }

    @Override
    public String read(String key) throws Exception {
        // First check local storage
        if (key.startsWith("N:") && addressPair.containsKey(key)) {
            return addressPair.get(key);
        }
        if (key.startsWith("D:") && dataPair.containsKey(key)) {
            return dataPair.get(key);
        }

        // If not found locally, query the network
        List<String> closestNodes = closestNodesAlgorithm(key, 3);
        String transactionID = generateTransactionID;

        for (String node : closestNodes) {
            if (!node.equals(nodeName) && addressPair.containsKey(node)) {
                InetSocketAddress address = Helper.returnSocketAddress(addressPair.get(node));
                if (address != null) {
                    String request = transactionID + " R " + Helper.formatSpaces(key) + key + " ";
                    sendRequest(request, address.getAddress(), address.getPort(), "R");
                }
            }
        }

        // Wait for response with timeout
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 1000) {
            handleIncomingMessages(100);
            if (pendingResponses.containsKey(transactionID)) {
                return pendingResponses.remove(transactionID);
            }
        }

        return null;
    }

    @Override
    public boolean write(String key, String value) throws Exception {
        List<String> closestNodes = closestNodesAlgorithm(key, 3);
        String transactionID = generateTransactionID;
        int successfulWrites = 0;
        int requiredWrites = Math.min(3, closestNodes.size());

        for (String node : closestNodes) {
            if (!node.equals(nodeName) && addressPair.containsKey(node)) {
                InetSocketAddress address = Helper.returnSocketAddress(addressPair.get(node));
                if (address != null) {
                    String request = transactionID + " W " + Helper.formatSpaces(key) + key + " " + Helper.formatSpaces(value) + value + " ";
                    sendRequest(request, address.getAddress(), address.getPort(), "W");

                    // Wait for response with timeout
                    long startTime = System.currentTimeMillis();
                    while (System.currentTimeMillis() - startTime < 1000) {
                        handleIncomingMessages(100);
                        if (pendingResponses.containsKey(transactionID)) {
                            String response = pendingResponses.remove(transactionID);
                            if (response.equals("A") || response.equals("R")) {
                                successfulWrites++;
                            }
                            break;
                        }
                    }
                }
            }
        }

        // Also write locally if we're one of the closest nodes
        if (shouldStoreLocally(key)) {
            if (key.startsWith("N:")) {
                addressPair.put(key, value);
            } else {
                dataPair.put(key, value);
            }
            successfulWrites++;
        }

        return successfulWrites >= Math.min(2, requiredWrites); // At least 2 successful writes
    }

    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        List<String> closestNodes = closestNodesAlgorithm(key, 3);
        String transactionID = generateTransactionID;

        for (String node : closestNodes) {
            if (!node.equals(nodeName) && addressPair.containsKey(node)) {
                InetSocketAddress address = Helper.returnSocketAddress(addressPair.get(node));
                if (address != null) {
                    String request = transactionID + " C " + Helper.formatSpaces(key) + key + " " + Helper.formatSpaces(currentValue) + currentValue + " " + Helper.formatSpaces(newValue) + newValue + " ";
                    sendRequest(request, address.getAddress(), address.getPort(), "C");

                    long startTime = System.currentTimeMillis();
                    while (System.currentTimeMillis() - startTime < 1000) {
                        handleIncomingMessages(100);
                        if (pendingResponses.containsKey(transactionID)) {
                            String response = pendingResponses.remove(transactionID);
                            if (response.equals("R") || response.equals("A")) {
                                return true;
                            }
                            break;
                        }
                    }
                }
            }
        }
        if (shouldStoreLocally(key)) {

                if (key.startsWith("D:")) {
                    if (dataPair.containsKey(key) && dataPair.get(key).equals(currentValue)) {
                        dataPair.put(key, newValue);
                        return true;
                    } else if (!dataPair.containsKey(key) && currentValue.isEmpty()) { // Special case for creating if it doesn't exist
                        dataPair.put(key, newValue);
                        return true;
                    }
                } else if (key.startsWith("N:")) {
                    if (addressPair.containsKey(key) && addressPair.get(key).equals(currentValue)) {
                        addressPair.put(key, newValue);
                        return true;
                    } else if (!addressPair.containsKey(key) && currentValue.isEmpty()) { // Special case for creating if it doesn't exist
                        addressPair.put(key, newValue);
                        return true;
                    }
                }
        }
        return false;
    }

    private List<String> closestNodesAlgorithm(String key, int count) throws Exception {
        String targetHash;
        try {
            targetHash = Helper.fromBytesToHexFormat(HashID.computeHashID(key));
        } catch (Exception e) {
            throw new Exception("Error computing hash for key: " + key, e);
        }

        Map<String, Integer> distances = new HashMap<>();
        for (String node : addressPair.keySet()) {
            String nodeHash = Helper.fromBytesToHexFormat(HashID.computeHashID(node));
            distances.put(node, Helper.computeHashDistance(targetHash, nodeHash));
        }

        return distances.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .limit(count)
                .map(Map.Entry::getKey)
                .collect(java.util.stream.Collectors.toList());
    }

    private synchronized void addDistance(int distance, String nodeName) {
        // Get or create the list for this distance
        List<String> nodesAtDistance = addressOfCalculatedDistances.computeIfAbsent(distance, k -> new ArrayList<>());

        // Add the node if not already present
        if (!nodesAtDistance.contains(nodeName)) {
            nodesAtDistance.add(nodeName);

            // Enforce maximum of 3 nodes per distance
            if (nodesAtDistance.size() > 3) {
                // Remove the oldest node (FIFO)
                nodesAtDistance.remove(0);
            }
        }
    }

    private boolean isKeyOfThreeClosest(String key) {
        try {
            List<String> closestNodes = closestNodesAlgorithm(key, 3);
            return closestNodes.contains(this.nodeName);
        } catch (Exception e) {
            return false;
        }
    }


    private boolean shouldStoreLocally(String key) throws Exception {
        String targetHash = Helper.fromBytesToHexFormat(HashID.computeHashID(key));
        String ourHash = Helper.fromBytesToHexFormat(HashID.computeHashID(this.nodeName));
        int ourDistance = Helper.computeHashDistance(targetHash, ourHash);

        // Check if we're among the 3 closest nodes
        List<String> closestNodes = closestNodesAlgorithm(key, 3);
        return closestNodes.contains(this.nodeName);
    }
}

