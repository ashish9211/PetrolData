
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


public class RateDiff {
	
	static class MyMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>
	{
		public void map(LongWritable key,Text Value,Context context) throws IOException, InterruptedException
		{
			String str[]=Value.toString().split(",");
			StringBuilder sb=new StringBuilder(str[3]);
			String sr=sb.deleteCharAt(0).toString();
			StringBuilder sb1=new StringBuilder(str[2]);
			String br=sb1.deleteCharAt(0).toString();
			double diff=Double.parseDouble(sr)-Double.parseDouble(br);
			context.write(new Text(str[6]),new DoubleWritable(diff));
		}
	}
	 
	static class MyReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	{
		public void reduce (Text key,DoubleWritable value,Context context)
		{
			
			try {
				context.write(key,value);
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
	Job job=Job.getInstance(conf, "RateDiff");
	job.setJarByClass(RateDiff.class);
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