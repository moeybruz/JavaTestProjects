public class SecondMinute {
    public static String getDurationString(int minutes, int seconds) {
        if (minutes < 0 || ((seconds < 0) || (seconds > 59))) {
            return "Invalid value";
        } else {
            int hours = minutes / 60;
            int reminute = minutes % 60;
            return hours + "h " + reminute + "m " + seconds + "s";
        }
    }

    public static String getDurationString(int seconds) {
        if (seconds < 0) {
            return "Invalid value";
        } else {
            int minutes = seconds / 60;
            int remaining = seconds % 60;
            return getDurationString(minutes, remaining);
        }
    }
}
