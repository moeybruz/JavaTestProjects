public class DecimalComparator {
    public static boolean areEqualByThreeDecimalPlaces(double x, double y){
        if ((int)(x*1000)==(int)(y*1000)){
            return true;
        } return false;
    }
}
