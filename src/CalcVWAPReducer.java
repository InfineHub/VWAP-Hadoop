import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.mapreduce.Reducer;

public class CalcVWAPReducer extends
Reducer<IsinKey, Tick, IsinKey, OutputVWAP> {
	private String isin = null;
	private float vwap = 0.0f;
	private long totalVolume = 0;
	private List<Date> hoursList = null;
	private SimpleDateFormat f = new SimpleDateFormat("HH:mm:ss");

	protected void reduce(IsinKey key, Iterable<Tick> values,
			Context context) throws IOException, InterruptedException {

		IsinKey lastKey = null;
		OutputVWAP lastOutput = null;
		int currIdx = 0;

		if (hoursList == null) {
			hoursList = new ArrayList<Date>();
			String []dates = context.getConfiguration().getStrings("DATES");
			for (int i = 0; i < dates.length; i++) {
				try {
					hoursList.add(f.parse(dates[i]));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		for (Tick value : values) {
			try {
				if (currIdx >= hoursList.size()) {
					return;
				}
				if (f.parse(key.getDate()).after(hoursList.get(currIdx))) {

					context.write(lastKey, lastOutput);
					if (currIdx < hoursList.size()-1) {
						currIdx++;
					} else {
						return;
					}
				}

				if (isin == null || !isin.equals(key.getKey())) {
					isin = key.getKey();
					vwap = 0.0f;
					totalVolume = 0;
				}

				Float price = Float.parseFloat(value.getPrice());
				Integer volume = Integer.parseInt(value.getVolume());
				vwap = (volume * price + totalVolume * vwap)
						/ (totalVolume + volume);
				totalVolume += volume;

				lastKey = new IsinKey(key.getKey(), f.format(hoursList.get(currIdx)));
				lastOutput = new OutputVWAP(vwap);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		context.write(lastKey, lastOutput);
	}
}