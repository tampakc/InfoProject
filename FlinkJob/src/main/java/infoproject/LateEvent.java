package infoproject;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "infosystems", name = "late")
public class LateEvent {
    @Column(name = "timestamp")
    private long timestamp = 0L;

    @Column(name = "value")
    private int value = 0;

    public LateEvent() {}

    public LateEvent(long timein, int valin){
        timestamp = timein;
        value = valin;
    }
		/*public LateEvent(Date timestamp, int value) {
			this.setTimestamp(timestamp);
			this.setValue(value);
		}*/

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timein) {
        timestamp = timein;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int valin) {
        value = valin;
    }

    public String toString() {
        return timestamp + " : " + value;
    }
}