package SecondProgram;

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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopTenRating
{
	
	// Driver program
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// get all args
		if (otherArgs.length != 2) 
		{
			System.err.println("Incompatible Number Of Arguments");
			System.exit(2);
		}

		// create a job with name "toptenrating"
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "toptenrating");

		job.setJarByClass(TopTenRating.class);
		job.setMapperClass(TopMapper.class);
		job.setReducerClass(TopReducer.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(FloatWritable.class);
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		
		FileInputFormat.setMinInputSplitSize(job, 500000000);

		System.exit(job.waitForCompletion(true) ? 1 : 0);
	}

	
	public static class TopMapper extends Mapper<LongWritable, Text, Text, FloatWritable> 
	{
		static String tot_records = "";

		@Override
		protected void map(LongWritable baseAddress, Text line, Context context) throws IOException, InterruptedException 
		{
			Text business_id = new Text();	// type of output key
			FloatWritable stars = new FloatWritable(1);

			tot_records = tot_records.concat(line.toString());
			String[] fields = tot_records.split("::");
			
			business_id.set(fields[2].trim());
			double d=Double.parseDouble(fields[3].trim());
			String f=d+"";
			float e = Float.parseFloat(f);
			stars.set(e);
			context.write(business_id, stars);
			tot_records = "";
		}
	}

	public static class TopReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> 
	{
		HashMap<String, Float> map = new HashMap<String, Float>();

		@Override
		protected void reduce(Text business_id, Iterable<FloatWritable> stars, Context context) throws IOException, InterruptedException 
		{
			FloatWritable average = new FloatWritable(0);
			float total = 0;
			float count = 0;
			
			for (FloatWritable star : stars) 
			{
				total += star.get();
				count++;
			}
	
			float avg = total / count;
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
				context.write(new Text(entry.getKey()),new FloatWritable(entry.getValue()));	// create a pair <key,value>
				i++;
				if (i == 10)
					break;
			}
		}
	}


}