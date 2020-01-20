public class Calculations {
    public double bmicalc() {
        BmiCalc bmiCalc = new BmiCalc();
        double newVal = bmiCalc.heightInput();
        double bmicalc = bmiCalc.weightInput() / (newVal * newVal);
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

        return bmicalc;
    }
}
