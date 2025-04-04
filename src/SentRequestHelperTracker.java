import java.net.InetAddress;

public class SentRequestHelperTracker {
        byte[] requestData;
        InetAddress destinationAddress;
        int destinationPort;
        String requestType;

        public SentRequestHelperTracker(byte[] requestData, InetAddress destinationAddress,
                                        int destinationPort, String requestType) {
            this.requestData = requestData;
            this.destinationAddress = destinationAddress;
            this.destinationPort = destinationPort;
            this.requestType = requestType;
        }
    }
