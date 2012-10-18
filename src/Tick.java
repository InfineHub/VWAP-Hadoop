import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class Tick implements WritableComparable {

	private String volume;
	private String price;
	
	
	public Tick() {
		// TODO Auto-generated constructor stub
	}
	
	public Tick(String volume, String price) {

		try {
			this.volume = volume;
			this.price = price;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public String getVolume() {
		return volume;
	}
	public void setVolume(String volume) {
		this.volume = volume;
	}
	public String getPrice() {
		return price;
	}
	public void setPrice(String price) {
		this.price = price;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		String line = in.readUTF();
		String[] lines = line.trim().split(";");

		if (lines.length>= 2) {
			volume = lines[0];
			price = lines[1];
		}
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(volume+";"+price);
	}
	@Override
	public int compareTo(Object arg0) {
	    String thisValue = this.price;
	    String thatValue = ((Tick)arg0).price;
	    return thisValue.compareTo(thatValue);
	}
	
	@Override
	public String toString() {
		return volume+";"+price;
	}
}
