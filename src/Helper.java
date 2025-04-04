import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

public class Helper {
    public static String generateTransactionID() {
        char first = (char) (65 + (int) (Math.random() * 26));
        char second = (char) (65 + (int) (Math.random() * 26));
        return "" + first + second + " ";
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
    public static String checkForSpaces(String input) {
        if (input == null) return "0 ";
        int spaceCount = input.length() - input.replace(" ", "").length();
        return spaceCount + " ";
    }

    public static String dataKeyFormat(String s) {
        return "D:" + s;
    }

    public static String[] relayMessageParsing(String str) {
        String[] splitNumberAway = str.split(" ",2);
        //    System.out.println(splitNumberAway[0]);

        int spaces = Integer.parseInt(splitNumberAway[0]);
        //    System.out.println(spaces);

        String restOfString = splitNumberAway[1];
        //    System.out.println(restOfString);

        String[] finalForm = new String[2];
        int countOfSpaces = 0;
        for (int i = 0; i < restOfString.length(); i++) {
            if (restOfString.charAt(i) == ' ') {
                countOfSpaces++;
            }
            if (countOfSpaces == spaces + 1) {
                finalForm[0] = restOfString.substring(0 , i);
                finalForm[1] = restOfString.substring(i+1);
                break;
            }
        }
        return finalForm;
    }

    public static InetSocketAddress returnSocketAddress(String address) {
        try {
            String[] parts = address.split(":");
            if (parts.length == 2) {
                return new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
            }
        } catch (Exception e) {
            System.err.println("Error parsing address: " + address + " - " + e.getMessage());
        }
        return null;
    }

    public static List<String> parseSpacedFields(String message) {
        List<String> result = new ArrayList<>();
        String[] parts = message.split(" ");
        int i = 0;

        while (i < parts.length) {
            try {
                int spaceCount = Integer.parseInt(parts[i]);
                StringBuilder field = new StringBuilder();

                for (int j = 1; j <= spaceCount + 1 && i + j < parts.length; j++) {
                    if (j > 1) field.append(" ");
                    field.append(parts[i + j]);
                }

                result.add(field.toString());
                i += spaceCount + 2;
            } catch (NumberFormatException e) {
                result.add(parts[i]);
                i++;
            }
        }

        return result;
    }

    public static int computeHashDistance(String hash1, String hash2) {
        int distance = 0;
        int length = Math.min(hash1.length(), hash2.length());

        for (int i = 0; i < length; i++) {
            int val1 = Integer.parseInt(hash1.substring(i, i+1), 16);
            int val2 = Integer.parseInt(hash2.substring(i, i+1), 16);
            int xor = val1 ^ val2;

            if (xor == 0) {
                distance += 4;
            } else {
                distance += Integer.numberOfLeadingZeros(xor) - 28;
                break;
            }
        }

        return 256 - Math.min(distance, 256);
    }

    public static String formatSpaces(String input) {
        if (input == null) return "0 ";
        int spaceCount = input.length() - input.replace(" ", "").length();
        return spaceCount + " ";
    }

    public static String fromBytesToHexFormat(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }


    public static void main(String[] args) {
        String str1 = "0 N:test 0 192.168.1.117:20111 ";
        System.out.println(Helper.parseSpacedFields(str1));
    }

}
