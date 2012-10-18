import java.text.SimpleDateFormat;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
	protected CompositeKeyComparator() {
		super(IsinKey.class, true);
	}	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		IsinKey k1 = (IsinKey)w1;
		IsinKey k2 = (IsinKey)w2;
		
		SimpleDateFormat f = new SimpleDateFormat("HH:mm:ss");
		
		int result = k1.getKey().compareTo(k2.getKey());
		if(0 == result) {
			try {
			result = f.parse(k1.getDate()).compareTo(f.parse(k2.getDate()));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return result;
	}
}