package fragmadata.question3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OverBowlReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable intWritable : values) {
			sum += intWritable.get();
		}
		context.write(new Text(key.toString() + "," + sum), NullWritable.get());

	}

}
