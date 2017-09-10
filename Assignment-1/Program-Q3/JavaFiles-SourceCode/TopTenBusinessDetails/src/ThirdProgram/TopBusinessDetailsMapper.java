package ThirdProgram;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopBusinessDetailsMapper extends Mapper<LongWritable, Text, Text, Text>
{

	static String tot_records = "";

	@Override
	protected void map(LongWritable baseAddress, Text line, Context context) throws IOException, InterruptedException 
	{

		Text businessid = new Text();
		Text alldetails = new Text();

		tot_records = tot_records.concat(line.toString());
		String[] field_data = tot_records.split("::");
		
		String full_address=field_data[1].trim();
		String categories=field_data[2].trim();
		String value="b:\t"+full_address+"\t"+categories;
				
		businessid.set(field_data[0].trim());
		alldetails.set(value);
		context.write(businessid, alldetails);
		tot_records="";
	}
}