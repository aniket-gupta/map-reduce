package fragmadata.question3;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OverAndRunCountReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int over = 0;
		int totalRun = 0;
		for(IntWritable intWritable : values) {
			totalRun += intWritable.get();
			over++;
		}
		
		if(over >= 10) {
			float economy =  totalRun/(float)over;
			String ecoStr = new DecimalFormat("0.00").format(economy);
			context.write(new Text(key.toString() + "," + ecoStr), NullWritable.get());
		}
		
	}

}
