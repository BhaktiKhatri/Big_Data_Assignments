package ThirdProgram;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopTenBusinessReducer extends Reducer<Text, Text, Text, Text> 
{

	@Override
	protected void reduce(Text businessid, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException 
	{
		boolean flag = false;

		String dataAstring = "";
		String dataBstring = "";
		for (Text value : values) 
		{
			if (value.toString().contains("a:")) 
			{
				dataAstring = value.toString();
				flag = true;
			}
			else
				dataBstring = value.toString();
		}
		
		if (!flag)
			return;
		
		
		String[] dataB = dataBstring.split("\t");
		String[] dataA = dataAstring.split("\t");
		String data = dataB[1] + "\t \t" + dataB[2] + "\t \t" + dataA[1];
		context.write(businessid, new Text(data));
	}
}