package Task7;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Sales 
{
	public static enum myCounters
	{
		INVALIDRECORDS,
		VALIDRECORDS
	}

	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		if(args==null || args.length!=2)
		{
			System.err.println("Incorrect input parameters provided");
			System.exit(-1);
		}
	
		
		Configuration conf = new Configuration();
		
		Job job  = new Job(conf,"Sales");
		
	
		job.setJarByClass(Sales.class);
	
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		job.setMapperClass(SalesMapper.class);
		job.setReducerClass(SalesReducer.class);
		job.setPartitionerClass(SalesPartitioner.class);
		
		job.setNumReduceTasks(5);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Television.class);
		job.setMapOutputValueClass(LongWritable.class);

		
		job.setOutputKeyClass(Television.class);
		job.setOutputValueClass(LongWritable.class);
				
		job.waitForCompletion(true);
	
		Counters counters  = job.getCounters();
		Counter counter  = counters.findCounter(myCounters.INVALIDRECORDS);
		System.out.println("Number of invalid records is "+counter.getValue());
		
		counter  = counters.findCounter(myCounters.VALIDRECORDS);
		System.out.println("Number of invalid records is "+counter.getValue());
	}

	
}
