package fragmadata.question3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import fragmadata.common.DeliveriesMapTask;
import fragmadata.common.JoiningReduceTask;
import fragmadata.common.MatchesMapTask;
import fragmadata.common.OverwritingTextOutputFormat;
import fragmadata.question2.TotalTeamRunInYear;

public class YearwiseTop10Bowler extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new YearwiseTop10Bowler(), args);
		System.exit(exitCode);

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf1 = getConf();
		Configuration conf2 = getConf();
		Configuration conf3 = getConf();
		Configuration conf4 = getConf();

		String[] programArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		if (programArgs.length != 3) {
			System.err.println("Usage: YearwiseTop10Bowler <in-Matches> <in-Deliveries> <out>");
			System.exit(2);
		}
		Path inputMathcesPath = new Path(programArgs[0]);
		Path inputDelPath = new Path(programArgs[1]);
		Path outputPath = new Path(programArgs[2]);
		String joinedDataset = "joined-dataset";
		Path joinedDatasetPath = new Path(joinedDataset);
		if (programArgs[2].lastIndexOf("/") != -1) {
			joinedDatasetPath = new Path(
					programArgs[1].substring(0, programArgs[1].lastIndexOf("/") + 1) + joinedDataset);
		}
		String overBowlsRun = "over-bowls-run";
		Path overBowlsRunPath = new Path(overBowlsRun);
		if (programArgs[2].lastIndexOf("/") != -1) {
			overBowlsRunPath = new Path(
					programArgs[1].substring(0, programArgs[1].lastIndexOf("/") + 1) + overBowlsRun);
		}
		String overRunCount = "over-run-count";
		Path overRunCountPath = new Path(overRunCount);
		if (programArgs[2].lastIndexOf("/") != -1) {
			overRunCountPath = new Path(
					programArgs[1].substring(0, programArgs[1].lastIndexOf("/") + 1) + overRunCount);
		}

		Job joinDatasetJob = new Job(conf1, "Join data set");
		joinDatasetJob.setJarByClass(TotalTeamRunInYear.class);
		joinDatasetJob.setReducerClass(JoiningReduceTask.class);
		joinDatasetJob.setMapOutputKeyClass(IntWritable.class);
		joinDatasetJob.setMapOutputValueClass(Text.class);
		joinDatasetJob.setOutputKeyClass(Text.class);
		joinDatasetJob.setOutputValueClass(NullWritable.class);
		joinDatasetJob.setOutputFormatClass(OverwritingTextOutputFormat.class);
		MultipleInputs.addInputPath(joinDatasetJob, inputMathcesPath, TextInputFormat.class, MatchesMapTask.class);
		MultipleInputs.addInputPath(joinDatasetJob, inputDelPath, TextInputFormat.class, DeliveriesMapTask.class);
		FileOutputFormat.setOutputPath(joinDatasetJob, joinedDatasetPath);

		Job overBowlsRunJob = Job.getInstance(conf2, "overBowlsRunJob");
		overBowlsRunJob.setOutputFormatClass(OverwritingTextOutputFormat.class);
		overBowlsRunJob.setJarByClass(YearwiseTop10Bowler.class);
		overBowlsRunJob.setMapperClass(OverBowlMapper.class);
		overBowlsRunJob.setReducerClass(OverBowlReducer.class);
		overBowlsRunJob.setMapOutputKeyClass(Text.class);
		overBowlsRunJob.setMapOutputValueClass(IntWritable.class);
		overBowlsRunJob.setOutputKeyClass(Text.class);
		overBowlsRunJob.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(overBowlsRunJob, joinedDatasetPath);
		FileOutputFormat.setOutputPath(overBowlsRunJob, overBowlsRunPath);

		Job overRunCountJob = Job.getInstance(conf3, "overRunCount");
		overRunCountJob.setOutputFormatClass(OverwritingTextOutputFormat.class);
		overRunCountJob.setJarByClass(YearwiseTop10Bowler.class);
		overRunCountJob.setMapperClass(OverAndRunCountMapper.class);
		overRunCountJob.setReducerClass(OverAndRunCountReducer.class);
		overRunCountJob.setMapOutputKeyClass(Text.class);
		overRunCountJob.setMapOutputValueClass(IntWritable.class);
		overRunCountJob.setOutputKeyClass(Text.class);
		overRunCountJob.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(overRunCountJob, overBowlsRunPath);
		FileOutputFormat.setOutputPath(overRunCountJob, overRunCountPath);

		Job top10BowlerJob = Job.getInstance(conf4, "top10BowlerJob");
		top10BowlerJob.setOutputFormatClass(OverwritingTextOutputFormat.class);
		top10BowlerJob.setJarByClass(YearwiseTop10Bowler.class);
		top10BowlerJob.setMapperClass(Top10BowlerMapper.class);
		top10BowlerJob.setReducerClass(Top10BowlerReducer.class);
		top10BowlerJob.setMapOutputKeyClass(Text.class);
		top10BowlerJob.setMapOutputValueClass(Text.class);
		top10BowlerJob.setOutputKeyClass(Text.class);
		top10BowlerJob.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(top10BowlerJob, overRunCountPath);
		FileOutputFormat.setOutputPath(top10BowlerJob, outputPath);

		// overBowlsRunJob
		// overRunCountJob
		// top10BowlerJob

		ControlledJob controlledJob1 = new ControlledJob(conf1);
		controlledJob1.setJob(joinDatasetJob);

		ControlledJob controlledJob2 = new ControlledJob(conf2);
		controlledJob2.setJob(overBowlsRunJob);
		controlledJob2.addDependingJob(controlledJob1);

		ControlledJob controlledJob3 = new ControlledJob(conf3);
		controlledJob3.setJob(overRunCountJob);
		controlledJob3.addDependingJob(controlledJob2);

		ControlledJob controlledJob4 = new ControlledJob(conf4);
		controlledJob4.setJob(top10BowlerJob);
		controlledJob4.addDependingJob(controlledJob3);

		JobControl jobControl = new JobControl("Job-Control");
		jobControl.addJob(controlledJob1);
		jobControl.addJob(controlledJob2);
		jobControl.addJob(controlledJob3);
		jobControl.addJob(controlledJob4);

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
		return (joinDatasetJob.waitForCompletion(true) ? 0 : 1);
	}

}
