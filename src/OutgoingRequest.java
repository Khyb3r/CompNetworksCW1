import java.net.InetAddress;

public class OutgoingRequest {
    String transactionID;
    InetAddress inetAddress;
    int retries;
    long currentTime;
    String message;
    public OutgoingRequest(String transactionID, InetAddress destAddress, String message) {
        this.transactionID = transactionID;
        this.inetAddress = destAddress;
        this.message = message;
        this.currentTime = System.currentTimeMillis();
        this.retries = 0;
    }
}
