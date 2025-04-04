import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Helper {
    public static String generateTransactionID() {
        char first = (char) (65 + (int) (Math.random() * 26));
        char second = (char) (65 + (int) (Math.random() * 26));
        return "" + first + second + " ";
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

    public static byte[] HashIDStringToBytes(String s) {
        byte[] targetHash = new byte[32];
        for (int i = 0; i < 32; i++) {
            targetHash[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
        }
        return targetHash;
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

    public static List<String> parseSpacedFields(String message) {
        List<String> parsedFields = new ArrayList<>();
        String[] pieces = message.split(" ");
        int i = 0;

        while (i < pieces.length) {
            try {
                int numSpaces = Integer.parseInt(pieces[i]);
                int expectedPieces = numSpaces + 1;

                if (i + expectedPieces >= pieces.length) {
                    break;
                }

                StringBuilder builder = new StringBuilder();
                for (int j = 0; j < expectedPieces; j++) {
                    if (j > 0) {
                        builder.append(" ");
                    }
                    builder.append(pieces[i + 1 + j]);
                }
                parsedFields.add(builder.toString());
                i += expectedPieces + 1;

            } catch (NumberFormatException e) {
                parsedFields.add(pieces[i]);
                i++;
            } catch (ArrayIndexOutOfBoundsException e) {
                break;
            }
        }
        return parsedFields;
    }

    public static void main(String[] args) {
        String str1 = "0 N:test 0 192.168.1.117:20111 ";
        System.out.println(Helper.parseSpacedFields(str1));
    }

}
