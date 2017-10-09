package Task7;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesReducer extends Reducer<Television,LongWritable,Television,LongWritable>
{
	LongWritable sales;
	long lCount;
	
	@Override
	public void setup(Context context)
	{
		sales = new LongWritable();
	}
	
	@SuppressWarnings("unused")
	@Override
	public void reduce(Television television,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException
	{
		System.out.println("Reducer input : key = "+television.toString());
		lCount = 0;
		for(LongWritable value:values)
		{
			lCount++;
		}

		System.out.println("Reducer input : key = "+television.toString() +"had "+lCount+" values");
		sales.set(lCount);
		context.write(television, sales);
	}
}
