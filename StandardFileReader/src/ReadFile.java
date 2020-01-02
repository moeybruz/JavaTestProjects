import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Scanner;

public class ReadFile {

    public static void main(String[] args) throws IOException
    {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Input file name: ");
        String file_name = scanner.nextLine();
        scanner.close();

        HashMap<String, String> map = new HashMap<>();

        BufferedReader inputStream = new BufferedReader(new FileReader(file_name));
        String line = inputStream.readLine();
        while(line != null) {
            String[] parts = line.split(":", 2);
            if (parts.length >= 2)
            {
                String key = parts[0];
                String value = parts[1];
                map.put(key, value);
            } else {
                System.out.println("Ignoring line: " +line);
            }

            line = inputStream.readLine();
        }

        for (String key: map.keySet()){
            System.out.println(key + " and " + map.get(key));
        }
        inputStream.close();

    }
}
