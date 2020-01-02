public class MinutesToYearsDaysCalculator {

    public static void main(String[] args) {
        printYearsAndDays(-561600);
    }

    public static void printYearsAndDays(long minutes) {
        if (minutes < 0) {
            System.out.println("Invalid Value");
        } else {
            long years = minutes / 525600;
            long reminMins = minutes % 525600;
            long days = reminMins / 1440;


            System.out.println(minutes + " min = " + years + " y and " + days + " d");
        }


    }
}
