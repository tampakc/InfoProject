package infoproject;

public class TimeSeriesData extends Object {
    public long timestamp;
    public int value;

    public long getTimestamp() {
        return timestamp;
    }

    public int getValue() {
        return value;
    }

    public void setTimestamp(long time) {
        timestamp = time;
    }

    public void setValue(int val) {
        value = val;
    }

    TimeSeriesData(long time, int val) {
        timestamp = time;
        value = val;
    }

    public int getKey() {
        return value % 4;
    }
}
