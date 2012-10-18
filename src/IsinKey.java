import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class IsinKey implements WritableComparable<IsinKey>  {

	private String key;
	private String date;
	
	private SimpleDateFormat f = new SimpleDateFormat("HH:mm:ss");
	
	public IsinKey() {
		// TODO Auto-generated constructor stub
	}
	
	public IsinKey(String key, String date) {
		this.key = key;
		this.date = date;

	}
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		key = WritableUtils.readString(in);
		date = WritableUtils.readString(in);
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, key);
		WritableUtils.writeString(out, date);
		
	}
	@Override
	public int compareTo(IsinKey arg0) {
		int result = key.compareTo(arg0.key);
		if(0 == result) {
			try {
				result = f.parse(date).compareTo(f.parse(arg0.date));	
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		return result;
	}
	
	@Override
	public String toString() {
		return key+"-"+date;
	}
	
}
