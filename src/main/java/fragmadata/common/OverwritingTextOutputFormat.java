package fragmadata.common;

import java.io.IOException;

import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class OverwritingTextOutputFormat<K,V> extends TextOutputFormat<K, V> {

	@Override
	public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {
//		// TODO Auto-generated method stub
//		super.checkOutputSpecs(job);
	}

}
