package FourthProgram;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusinessFilterMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	static String total_record = "";

	@Override
	protected void map(LongWritable baseAddress, Text line, Context context) throws IOException, InterruptedException 
	{
		Text business_id = new Text();
		Text city = new Text();
		total_record = total_record.concat(line.toString());
		String[] fields = total_record.split("::");

		if ((fields[1].contains("Stanford"))) 
		{
			business_id.set(fields[0].trim());
			city.set(fields[1].trim());
			context.write(business_id, city);
		}
		total_record = "";
	}
}