import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner userInput = new Scanner(System.in);
        System.out.println("Enter the KB that needs to be converted to MB below.");
        int kbInput = userInput.nextInt();
        MegaBytesConverter.printMegaBytesAndKiloBytes(kbInput);
    }
}
