package ThirdProgram;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class TopTenBusinessJoin 
{

	// Driver program
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		// get all args
		if (otherArgs.length != 2) 
		{
			System.err.println("Incompatible Number Of Arguments");
			System.exit(2);
		}
		
		String inbusi="/user/brk160030/business.csv";
		Path inputFileBus = new Path(inbusi);
		
		// create a job with name "toptenbusinessjoin"
		@SuppressWarnings("deprecation")
		Job job1 = new Job(conf, "toptenbusinessjoin");

		job1.setJarByClass(TopTenBusinessJoin.class);

		Path inputFile = new Path(otherArgs[0]);
		Path outputFile = new Path(otherArgs[1]);
		Path intermidiateFile = new Path("output_data"); 

		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job1, inputFile);
		// set the HDFS path for the output of first reduce data which is input to map2
		FileOutputFormat.setOutputPath(job1, intermidiateFile);
		
		// set output key type
		job1.setOutputKeyClass(Text.class);
		// set output value type
		job1.setOutputValueClass(FloatWritable.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(FloatWritable.class);

		job1.setMapperClass(TopTenMapper.class);
		job1.setReducerClass(TopTenReducer.class);

		FileInputFormat.setMinInputSplitSize(job1, 500000000);

		job1.waitForCompletion(true);

		@SuppressWarnings("deprecation")
		Job job2 = new Job(conf, "Joiner");
		job2.setJarByClass(TopTenBusinessJoin.class);

		job2.setReducerClass(TopTenBusinessReducer.class);

		MultipleInputs.addInputPath(job2, intermidiateFile,TextInputFormat.class, TopTenBusinessMapper.class);
		
		MultipleInputs.addInputPath(job2, inputFileBus, TextInputFormat.class, TopBusinessDetailsMapper.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job2, outputFile);
		FileInputFormat.setMinInputSplitSize(job2, 500000000);

		job2.waitForCompletion(true);

		org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
		fs.delete(intermidiateFile, true);
	}

	public static class TopTenMapper extends Mapper<LongWritable, Text, Text, FloatWritable> 
	{
		static String tot_records = "";

		@Override
		protected void map(LongWritable baseAddress, Text line, Context context) throws IOException, InterruptedException 
		{

			Text businessid = new Text();
			FloatWritable ratings = new FloatWritable(1);

			tot_records = tot_records.concat(line.toString());
			String[] fields_data = tot_records.split("::");
					
			businessid.set(fields_data[2].trim());
			double d=Double.parseDouble(fields_data[3].trim());
			String f=d+"";
			float e = Float.parseFloat(f);
			ratings.set(e);
			context.write(businessid, ratings);
			tot_records="";
		}
	}

	public static class TopTenReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> 
	{
		HashMap<String, Float> map = new HashMap<String, Float>();
		@Override
		protected void reduce(Text business_id, Iterable<FloatWritable> ratings, Context context) throws IOException, InterruptedException 
		{

			FloatWritable average = new FloatWritable(0);
			float tot = 0;
			float cnt = 0;
			for (FloatWritable star : ratings) 
			{
				tot += star.get();
				cnt++;
			}

			float avg = tot / cnt;
			average.set(avg);
			map.put(business_id.toString(), avg);
		}

		@Override
		protected void cleanup(Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException 
		{
			Map<String, Float> sortedMap = new TreeMap<String, Float>(new CompareValue(map));
			sortedMap.putAll(map);
			int i = 0;
			for (Map.Entry<String, Float> entry : sortedMap.entrySet())
			{
				context.write(new Text(entry.getKey()),new FloatWritable(entry.getValue()));
				i++;
				if (i == 10)
					break;
			}
		}
	}

}