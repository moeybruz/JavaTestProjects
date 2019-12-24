import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;

public class Age {
    public static void main(String[] args) {
        Scanner dob = new Scanner(System.in);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("d/MM/yyyy");
        System.out.println("What is your date of birth? ");
        LocalDate YourDob = LocalDate.parse(dob.next(), formatter);

        LocalDate now = LocalDate.now();

        Period diff = Period.between(YourDob, now);

        System.out.println("You are " +diff +" years old");


    }
}
