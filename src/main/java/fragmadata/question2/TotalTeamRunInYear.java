package fragmadata.question2;

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

public class TotalTeamRunInYear extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new TotalTeamRunInYear(), args);
		System.exit(exitCode);

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf1 = getConf();
		Configuration conf2 = getConf();
		String[] programArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		if (programArgs.length != 3) {
			System.err.println("Usage: YearWiseTeamRun <in-Matches> <in-Deliveries> <out>");
			System.exit(2);
		}
		Path inputMathcesPath = new Path(programArgs[0]);
		Path inputDelPath = new Path(programArgs[1]);
		Path outputPath = new Path(programArgs[2]);
		String intermedRes = "joined-dataset";
		Path intermedResPath = new Path(intermedRes);
		if (programArgs[2].lastIndexOf("/") != -1) {
			intermedResPath = new Path(programArgs[1].substring(0, programArgs[1].lastIndexOf("/") + 1) + intermedRes);
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
		FileOutputFormat.setOutputPath(joinDatasetJob, intermedResPath);

		Job calTotRunJob = Job.getInstance(conf2,
				"Total number of fours, sixes, total score with respect to team and year.");
		calTotRunJob.setJarByClass(TotalTeamRunInYear.class);
		calTotRunJob.setMapperClass(YearTeamRunMapper.class);
		calTotRunJob.setReducerClass(YearTeamRunReducer.class);
		calTotRunJob.setMapOutputKeyClass(Text.class);
		calTotRunJob.setMapOutputValueClass(Text.class);
		calTotRunJob.setOutputKeyClass(Text.class);
		calTotRunJob.setOutputValueClass(NullWritable.class);
		calTotRunJob.setOutputFormatClass(OverwritingTextOutputFormat.class);
		FileInputFormat.setInputPaths(calTotRunJob, intermedResPath);
		FileOutputFormat.setOutputPath(calTotRunJob, outputPath);

		ControlledJob controlledJob1 = new ControlledJob(conf1);
		controlledJob1.setJob(joinDatasetJob);

		ControlledJob controlledJob2 = new ControlledJob(conf2);
		controlledJob2.setJob(calTotRunJob);
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
		return (joinDatasetJob.waitForCompletion(true) ? 0 : 1);
	}

}
