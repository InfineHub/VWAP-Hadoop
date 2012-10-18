import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class OutputVWAP implements WritableComparable<OutputVWAP> {

	
//	private Integer volume;
//	private Float price;
	private Float vwap;
	
	public OutputVWAP(Float vwap) {
//		this.volume = volume;
//		this.price = price;
		this.vwap = vwap;
	}
	
//	public Integer getVolume() {
//		return volume;
//	}
//	public void setVolume(Integer volume) {
//		this.volume = volume;
//	}
//	public Float getPrice() {
//		return price;
//	}
//	public void setPrice(Float price) {
//		this.price = price;
//	}
	public Float getVwap() {
		return vwap;
	}
	public void setVwap(Float vwap) {
		this.vwap = vwap;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
//		volume = in.readInt();
//		price = in.readFloat();
		vwap = in.readFloat();
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		
//		out.writeInt(volume);
//		out.writeFloat(price);
		out.writeFloat(vwap);		
	}
	@Override
	public int compareTo(OutputVWAP arg0) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public String toString() {
		return vwap.toString();
	}
}
