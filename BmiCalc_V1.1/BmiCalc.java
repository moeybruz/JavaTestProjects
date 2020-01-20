import java.util.InputMismatchException;
import java.util.Scanner;

public class BmiCalc {

    public double heightInput() {
        Scanner calcMyBmi = new Scanner(System.in);
        System.out.println("What is your height (in metre)?");

        while (true) { // Keep looping until a valid height is given
            try {
                double myHeight = calcMyBmi.nextDouble();
                if (myHeight < 1 || myHeight > 3) {
                    System.out.println("Not a valid height, please try again!");
                } else {
                    return myHeight;
                }
            } catch (InputMismatchException e) {
                System.out.println("Please try again: Enter your valid height in metre!");
                calcMyBmi.nextLine();
            }
        }

    }

    public double weightInput() {
        Scanner calcMyBmi = new Scanner(System.in);
        System.out.println("What is your weight (in KG)?");

        while (true) { // Keep looping until a valid weight is given
            try {
                double myWeight = calcMyBmi.nextDouble();
                if (myWeight < 30 || myWeight > 150) {
                    System.out.println("Not a valid weight, please try again!");
                } else {
                    return myWeight;
                }
            } catch (InputMismatchException e) {
                System.out.println("Please try again: Enter your valid weight in KG!");
                calcMyBmi.nextLine();
            }
        }
    }
}
