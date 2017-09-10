package FirstProgram;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PaloAltoBusiness
{

	public static class Map extends Mapper<LongWritable, Text, Text, NullWritable>
	{
		private static String tot_records = "";
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			tot_records = tot_records.concat(value.toString());
			String[] field_data = value.toString().split("::");
			
			if(field_data[1].contains("Palo Alto"))
			{
				String b[]=field_data[2].split("List");
				String finalcat="",word="";
				
				for(int i=0;i<b[1].length();i++)
				{
					char c=b[1].charAt(i);
					String ch=c+""; 
					
					if(ch.equalsIgnoreCase("(") || ch.equalsIgnoreCase(")"))
					{
						if(i==0 || i==(b[1].length()-1))
						{
						}
						else
						{
							finalcat=finalcat+ch;
						}
					}
					else
					{
						if(ch.equalsIgnoreCase(","))
						{
							if(finalcat.charAt(0)==' ')
							{
								String wd=finalcat.substring(1, finalcat.length());
								word=wd;
							}
							else
							{
								word=finalcat;
							}

							Text ctg = new Text(word);	// type of output key
							NullWritable nullOb = NullWritable.get();
							context.write(ctg, nullOb);		// create a pair <key, value>
							finalcat="";
						}
						else
						{
							finalcat=finalcat+ch+"";
							
							if(i==(b[1].length()-2))
							{
								if(finalcat.charAt(0)==' ')
								{
									String wd=finalcat.substring(1, finalcat.length());
									word=wd;
								}
								else
								{
									word=finalcat;
								}

								Text ctg = new Text(word);	
								NullWritable nullOb = NullWritable.get();
								context.write(ctg, nullOb);
							}
						}
						
					}
					
				}
			}
			tot_records="";
			
		}

	}
	
	public static class Reduce extends Reducer<Text,NullWritable,Text,NullWritable> 
	{
		public void reduce(Text key, Iterable<NullWritable> values,Context context) throws IOException, InterruptedException 
		{
			NullWritable nullr = NullWritable.get();
			context.write(key, nullr); // create a pair <keyword, number of occurences>
		}
	}
	
	// Driver program
		public static void main(String[] args) throws Exception 
		{
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			// get all args
			if (otherArgs.length != 2) 
			{
				System.err.println("Usage: WordCount <in> <out>");
				System.exit(2);
			}
			// create a job with name "wordcount"
			Job job = new Job(conf, "paloaltobusiness");
			job.setJarByClass(PaloAltoBusiness.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			// set output key type
			job.setOutputKeyClass(Text.class);
			// set output value type
			job.setOutputValueClass(NullWritable.class);
			//set the HDFS path of the input data
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			// set the HDFS path for the output
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			//Wait till job completion
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}

