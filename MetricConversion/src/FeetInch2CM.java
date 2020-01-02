public class FeetInch2CM {
    public static double calcFeelAndInchesToCentimeters(double feet, double inches){
        if ((feet < 0) || ((inches < 0) || (inches > 12))){
            System.out.println("Invalid entry!");
            return -1;
        }

        double centimeter = (feet * 12) * 2.54;
        centimeter += inches * 2.54;
        System.out.println(feet + " feet " + inches + " inches = "  + centimeter + " cm");
        return centimeter;
    }

    public static double calcFeelAndInchesToCentimeters(double inches){
        if (inches < 0 ){
            System.out.println("You have entered invalid inches value!");
            return -1;
        }

        double feet = (int) inches / 12;
        double remaining = (int) inches % 12;
        System.out.println(inches + " inches are equal to " + feet + " feet " + remaining + " inches");
        return calcFeelAndInchesToCentimeters(feet, remaining);
    }
}
