import java.util.InputMismatchException;
import java.util.Scanner;

public class Mybmicalc {
    private double myWeight;
    private double myHeight;

    public static void main(String[] args) {
        Mybmicalc bmiCalcObj = new Mybmicalc();
        bmiCalcObj.calculateBMI();
    }

    public void calculateBMI() {
        Scanner calcMyBmi = new Scanner(System.in);
        System.out.println("What is your height (in metre)?");

        boolean badHeightInput = true;
        while (badHeightInput == true) {
            try {
                myHeight = calcMyBmi.nextDouble();
                if (myHeight < 1 || myHeight > 3) {
                    System.out.println("Not a valid height, please try again!");
                } else {
                    badHeightInput = false;
                }
            } catch (InputMismatchException e) {
                System.out.println("Please try again: Enter your valid height in metre!");
                calcMyBmi.nextLine();
            }
        }

        System.out.println("What is your weight (in KG)?");

        boolean badWeightInput = true;
        while (badWeightInput == true) {
            try {
                myWeight = calcMyBmi.nextDouble();
                if  (myWeight < 30 || myWeight > 150)   {
                    System.out.println("Not a valid weight, please try again!");
                } else {
                    badWeightInput = false;
                }
            } catch (InputMismatchException e) {
                System.out.println("Please try again: Enter your valid weight in KG!");
                calcMyBmi.nextLine();
            }
        }

        double bmicalc = myWeight / (myHeight * myHeight);
        System.out.println("Your BMI is " + bmicalc);

        if (bmicalc < 18.50) {
            System.out.println("Action required: You are considered underweight and possibly malnourished. Please see a nutritionist.");
        } else if (bmicalc <= 24.90) {
            System.out.println("Congrats! You are within a healthy weight range for young and middle-aged adults, so keep it up and no action required for you.");
        } else if (bmicalc <= 29.90) {
            System.out.println("Action required: You are considered overweight. Please see a nutritionist and consider a diet chart.");
        } else if (bmicalc > 29.90) {
            System.out.println("Urgent action required: You are considered obese. Please see a nutritionist immediately.");
        }
    }


}
