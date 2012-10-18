// WordCountMapper.java
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CalcVWAPMapper extends Mapper<LongWritable, Text, IsinKey, Tick> {
	private String currFile = null;
	private String currDate = null;
	private String currHour = null;
	private String currIsin = null;
	private List<String> isins = null;
	
    public String getFileName(Context context) {
    	FileSplit fileSplit = (FileSplit)context.getInputSplit();
    	return fileSplit.getPath().getName().toString();
    }
    
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

    	if (!getFileName(context).equals(currFile)) {
    		currFile = getFileName(context);
    		currDate = currFile.replace(".txt", "");
    	}
    	
    	if (isins == null) {
    		isins = Arrays.asList(context.getConfiguration().getStrings("ISIN"));
    	}
    	
    	String line = value.toString();
        if (line.startsWith("N;")) {
        	String[] result = line.split(";");
        	if (result.length >= 5) {
        		if (isins.contains(result[1])) {
        		
        			if (!result[1].equals(currIsin)) {
        				currIsin = result[1];
        			}
        			if (!result[2].equals(currHour)) {
        				currHour = result[2];
        			}
        			String volume = result[3];
        			String price = result[4];

        			context.write(new IsinKey(currIsin+"-"+currDate, currHour), new Tick(volume, price));
        		}
        	}
        	
        }

    }
 }