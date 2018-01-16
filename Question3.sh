
//3)Which industry has the most number of Data Scientist positions?
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Question3 {
	static class question3Mapper extends Mapper < LongWritable, Text, Text, LongWritable > {
	    LongWritable one = new LongWritable(1);
	    public void map(LongWritable key, Text values, Context context) throws IOException,
	    InterruptedException {
	            String[] str = values.toString().split("\t");
	            if(str[1].equals("CERTIFIED"))
	            {
	            if (str[4].contains("DATA SCIENTIST")) {
	                Text answer = new Text(str[3]);
	                context.write(answer, one);
	            }
	        }}
	    }

	
	static class question3Reducer extends Reducer < Text, LongWritable, NullWritable, Text > {
	    private TreeMap < LongWritable,Text > top5industry4ds = new TreeMap < LongWritable,
	    Text > ();

	    public void reduce(Text key, Iterable < LongWritable > values, Context context) throws IOException,
	    InterruptedException {

	        long sum = 0;
	        for (LongWritable val: values)
	            sum += val.get();

	        top5industry4ds.put(new LongWritable(sum), new Text(key.toString()+"\t"+sum));
	        if (top5industry4ds.size() > 5)
	        	top5industry4ds.remove(top5industry4ds.firstKey());
	    }

	    protected void cleanup(Context context) throws IOException,
	    InterruptedException {
	        for (Text t: top5industry4ds.descendingMap().values())
	            context.write(NullWritable.get(), t);

	    }

	}
    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 5 Industries for Data Scientist jobs ");

        job.setJarByClass(Question3.class);
        job.setMapperClass(question3Mapper.class);
        job.setReducerClass(question3Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 1 : 0);

    }
}

