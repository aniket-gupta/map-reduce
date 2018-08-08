package fragmadata.question1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import fragmadata.common.OverwritingTextOutputFormat;

public class TopTeamMapReduce extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new TopTeamMapReduce(), args);
		System.exit(exitCode);

	}

	public int run(String[] args) throws Exception {
		Configuration conf1 = getConf();
		Configuration conf2 = getConf();
		String[] programArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		if (programArgs.length != 2) {
			System.err.println("Usage: TopTeamMapReduce <in> <out>");
			System.exit(2);
		}

		Path inputPath = new Path(programArgs[0]);
		Path outputPath = new Path(programArgs[1]);
		String intermedRes = "intermedRes";
		Path intermedResPath = new Path(intermedRes);
		if (programArgs[1].lastIndexOf("/") != -1) {
			intermedResPath = new Path(programArgs[1].substring(0, programArgs[1].lastIndexOf("/") + 1) + intermedRes);
		}

		Job job1 = Job.getInstance(conf1, "Teams which elected to field first after winning toss");
		job1.setJarByClass(TopTeamMapReduce.class);
		job1.setMapperClass(TossTeamMapper.class);
		job1.setReducerClass(TossTeamReducer.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(NullWritable.class);
		job1.setOutputFormatClass(OverwritingTextOutputFormat.class);

		FileInputFormat.addInputPath(job1, inputPath);
		FileOutputFormat.setOutputPath(job1, intermedResPath);

		Job job2 = Job.getInstance(conf2, "Top 4 Teams which elected to field first after winning toss");
		job2.setJarByClass(TopTeamMapReduce.class);
		job2.setMapperClass(Top4TeamMapper.class);
		job2.setReducerClass(Top4TeamReducer.class);
		job2.setMapOutputKeyClass(NullWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);
		job2.setOutputFormatClass(OverwritingTextOutputFormat.class);
		FileInputFormat.addInputPath(job2, intermedResPath);
		FileOutputFormat.setOutputPath(job2, outputPath);

		ControlledJob controlledJob1 = new ControlledJob(conf1);
		controlledJob1.setJob(job1);

		ControlledJob controlledJob2 = new ControlledJob(conf2);
		controlledJob2.setJob(job2);
		controlledJob2.addDependingJob(controlledJob1);

		JobControl jobControl = new JobControl("Job-Control");
		jobControl.addJob(controlledJob1);
		jobControl.addJob(controlledJob2);

		Thread jobControlThread = new Thread(jobControl);
		jobControlThread.start();
		while (!jobControl.allFinished()) {
			System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());
			System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
			System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
			System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
			System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
			try {
				Thread.sleep(5000);
			} catch (Exception e) {

			}
		}
		System.exit(0);
		return (job1.waitForCompletion(true) ? 0 : 1);

	}

}
