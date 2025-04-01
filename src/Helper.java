public class Helper {
    public static String generateTransactionID() {
        char first = (char) (65 + (int) (Math.random() * 26));
        char second = (char) (65 + (int) (Math.random() * 26));
        return "" + first + second + " ";
    }
    public static byte[] getHashID(String nodeName) throws Exception {
        return HashID.computeHashID(nodeName);
    }
    public static int computeHashDistance(byte[] hash1, byte[] hash2) throws Exception {
        int matchingBits = 0;
        for (int i = 0; i < hash1.length; i++) {
            for (int j = 7; j >= 0; j--) {
                if (((hash1[i] >> j) & 1) == ((hash2[i] >> j) & 1)) {
                    matchingBits++;
                }
                else {
                    return 256 - matchingBits;
                }
            }
        }
        return 0;
    }
    public static String formatStringToCRNMessage(String s) {
        int spaces = 0;
        for (char c : s.toCharArray()) {
            if (c == ' ') {
                spaces++;
            }
        }
        return spaces + " " + s + " ";
    }

    public static String formatCRNMessageToString(String s) {
        String[] parts = s.split(" ", 2);
        if (parts.length > 2) {
            throw new IllegalArgumentException("String incorrectly formatted");
        }
        int spaceCount;
        try {
            spaceCount = Integer.parseInt(parts[0]);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return parts[1].trim();
    }

    public static String dataKeyFormat(String s) {
        return "D:" + s;
    }

    public static void main(String[] args) {
        String test = formatCRNMessageToString("1 Hello World! ");
        System.out.print(test);
        System.out.print(test);
    }

}
