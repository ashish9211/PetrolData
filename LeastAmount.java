
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LeastAmount {
	
	static class MyMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>
	{
		public void map(LongWritable key,Text Value,Context context) throws IOException, InterruptedException
		{
			String str[]=Value.toString().split(",");
			StringBuilder sb=new StringBuilder(str[3]);
			sb.deleteCharAt(0);
			context.write(new Text(str[1]),new DoubleWritable(Double.parseDouble(sb.toString())));
		}
	}
	 
	static class MyReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	{
		public void reduce (Text key,Iterable<DoubleWritable> value,Context context)
		{
			double temp=0;
			double min=0;
			for(DoubleWritable val:value)
			{
				temp=val.get();
				if(min==0)
				{
					min=val.get();
				}
				if(temp<min)
				{
					min=temp;
				}
				
			}
			try {
				context.write(key,new DoubleWritable(min));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
	Configuration conf =new Configuration();
	Job job=Job.getInstance(conf, "LeastAmount");
	job.setJarByClass(LeastAmount.class);
	job.setMapperClass(MyMapper.class);
	job.setReducerClass(MyReducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(DoubleWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DoubleWritable.class);
	FileInputFormat.addInputPath(job,new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	System.exit(job.waitForCompletion(true)? 0:1);
	}
}
