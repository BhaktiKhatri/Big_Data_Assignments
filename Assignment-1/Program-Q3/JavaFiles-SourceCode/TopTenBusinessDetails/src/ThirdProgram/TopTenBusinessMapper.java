package ThirdProgram;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TopTenBusinessMapper extends Mapper<LongWritable, Text, Text, Text> 
{

	@Override
	protected void map(LongWritable key, Text linedata, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException 
	{
		if(linedata.getLength()>0)
		{
			String[] fields_data=linedata.toString().split("\t");	
			String value= "a:\t"+fields_data[1];
			context.write(new Text(fields_data[0].trim()), new Text(value));
		}
	}
}