public class AreaCalculator {
    public static double area(double radius) {
        double circleArea = radius * radius * 3.14159265;
        if (radius < 0) {
            return -1.0;
        }

        return circleArea;
    }

    public static double area(double x, double y) {
        double areaRectangle = x * y;
        if (x < 0 || y < 0) {
            return -1.0;
        }
        return areaRectangle;
    }
}
