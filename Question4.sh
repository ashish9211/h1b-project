
// 4 Which top 5 employers file the most petitions each year?
import java.io.IOException;

import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Question4 {
  static class question4Mapper extends Mapper < LongWritable, Text, Text, LongWritable > {
	    LongWritable one = new LongWritable(1);
	    public void map(LongWritable key, Text value, Context context) throws IOException,
	    InterruptedException {
	       
	            String[] str = value.toString().split("\t");
	            Text answer = new Text(str[2]+"\t"+ str[7]);
	                context.write(answer, one);
	            
	        }
	    }
	

  static class question4Partitioner extends Partitioner < Text, LongWritable > {
    @Override
    public int getPartition(Text key, LongWritable value, int numReduceTasks) {
        String[] str = key.toString().split("\t");
        if (str[1].equals("2011"))
            return 0;
        if (str[1].equals("2012"))
            return 1;
        if (str[1].equals("2013"))
            return 2;
        if (str[1].equals("2014"))
            return 3;
        if (str[1].equals("2015"))
            return 4;
        if (str[1].equals("2016"))
            return 5;
        else
            return 6;


    }
}
 
 static class question4Reducer extends Reducer < Text, LongWritable, NullWritable, Text > {
    private TreeMap < LongWritable,Text > Top5Employers = new TreeMap < LongWritable,
    Text > ();
    long sum = 0;
    public void reduce(Text key, Iterable < LongWritable > values, Context context) throws IOException,
    InterruptedException {
        sum = 0;
        for (LongWritable val: values) {
            sum += val.get();
        }
        Top5Employers.put(new LongWritable(sum), new Text(key + "," + sum));
        if (Top5Employers.size() > 5)
            Top5Employers.remove(Top5Employers.firstKey());

    }
    protected void cleanup(Context context) throws IOException,
    InterruptedException {
        for (Text t: Top5Employers.descendingMap().values())
            context.write(NullWritable.get(), t);

    }


}

    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException

    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top  5 Employers");

        job.setJarByClass(Question4.class);
        job.setMapperClass(question4Mapper.class);
        job.setPartitionerClass(question4Partitioner.class);
        job.setReducerClass(question4Reducer.class);

        job.setNumReduceTasks(7);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}

