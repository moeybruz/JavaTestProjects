
import java.io.*;
import java.util.Scanner;

public class FileData {
    public static void main(String[] args) throws IOException {

        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter file name: ");
        String file_name = scanner.nextLine();
        scanner.close();


        //Update the directory as per your requirement

        try {
            BufferedReader inputStream = new BufferedReader(new FileReader(file_name));
            String line = inputStream.readLine();
            while(line != null) {
                System.out.println(line);
                line = inputStream.readLine();
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}


//instructions followed: https://www.homeandlearn.co.uk/java/read_a_textfile_in_java.html
//Except from FileData.java Try part.
