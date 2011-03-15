package ir.assignment06;

import ir.assignment06.input.NgramsInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Ngrams extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		//args[0] = input path
		//args[1] = output path
		Configuration conf = new Configuration();
		
//		DistributedCache.addFileToClassPath(new Path("/user/flindenberg-pc/f.lindenberg/bliki-core-3.0.16.jar"), conf);
		DistributedCache.addFileToClassPath(new Path("/user/fabian/bliki-core-3.0.16.jar"), conf);

		Job job = new Job(conf, "ngrams");
		job.setJarByClass(Ngrams.class);

		job.setMapperClass(NgramsMapper1.class);
		job.setReducerClass(NgramsReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(NgramsInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return (job.waitForCompletion(true) ? 0: 1);
	}

	public static void main(String[] args) throws Exception {
//		args = new String[]{"data/input", "data/output"}; // TODO: delete this before running as jar 
		int res = ToolRunner.run(new Configuration(), new Ngrams(), args);
		System.exit(res);
	}

}
