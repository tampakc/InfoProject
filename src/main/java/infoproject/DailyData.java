package infoproject;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

import java.util.Date;
import java.text.SimpleDateFormat;

@Table(keyspace = "infosystems", name = "daily")
public class DailyData {

    static String pattern = "yyyy-MM-dd";
    static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);


    @Column(name = "day")
    private String day = null;

    @Column(name = "sum")
    private int sum = 0;

    @Column(name = "min")
    private int min = Integer.MAX_VALUE;

    @Column(name = "max")
    private int max = Integer.MIN_VALUE;

    @Column(name = "avg")
    private double avg = 0;

    public DailyData() {}

    public DailyData(Date dayin, int sumin, int minin, int maxin, double avgin) {
        day = simpleDateFormat.format(dayin);
        sum = sumin;
        min = minin;
        max = maxin;
        avg = avgin;
    }
		/*public DailyData(LocalDate day, int sum, int min, int max, double avg) {
			this.setDay(day);
			this.setSum(sum);
			this.setMin(min);
			this.setMax(max);
			this.setAvg(avg);
		} */

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    public int getMin() {
        return min;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

    public double getAvg() {
        return avg;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    @Override
    public String toString() {
        return getDay() + " : " + getSum() + " " + getMin() + " " + getMax() + " " + getAvg();
    }
}
