import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;




public class Question7 {
	static class question7Mapper extends Mapper < LongWritable, Text, Text, LongWritable > {
	   
	    public void map(LongWritable key, Text value, Context context) throws IOException,
	    InterruptedException {
	    	    LongWritable one = new LongWritable(1);
	       
	            String[] str = value.toString().split("\t");
	            String year= str[7];
	            context.write(new Text(year), one);
	            
	         }
	        }
	
	static class question7Reducer extends Reducer <Text, LongWritable, Text, LongWritable>
	{
		public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException
		{
		long sum=0;
		for(LongWritable v:value)
		{
		sum= sum +v.get() ;
					
		}
		LongWritable total= new LongWritable();
		total.set(sum);
		context.write(key,total);
		}
	}
	public static void main(String args[])  throws IOException, InterruptedException, ClassNotFoundException
	{
		  Configuration conf = new Configuration();
		  Job job = Job.getInstance(conf, "applications per year");

		  job.setJarByClass(Question7.class);
		  job.setMapperClass(question7Mapper.class);
		  job.setReducerClass(question7Reducer.class);
		  
		 

		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(LongWritable.class);
		    
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(LongWritable.class);
		  
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  System.exit(job.waitForCompletion(true) ? 0 : 1);

	  }

	

	    }
	

