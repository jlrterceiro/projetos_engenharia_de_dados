import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.Date;
import java.lang.*;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.text.DateFormat;


public class BitcoinSecondarySort {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable> {
	
		private final IntWritable one = new IntWritable(1);
		private static final String DATE_FORMAT = "yyyy-MM-dd";

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String []words = value.toString().split(",");
			if (words[0].equals("t")) return;
			Date date = new java.util.Date( Long.parseLong(words[0]));
			float h = Float.parseFloat(words[2]);
			DateFormat df = new SimpleDateFormat(DATE_FORMAT);
			String dateAsString = df.format(date);
			context.write(new Text(dateAsString), new FloatWritable(h));
		
		}
	
	}
	
	public static class IntSumReducer extends Reducer<Text, FloatWritable, Text, Text> {
		
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			String sum  = "";
			int counter = 0;
			for (FloatWritable val : values) {
				sum += Float.toString(val.get()) + " | ";
				counter++;
				if (counter==5) break;
			}
			context.write(key, new Text(sum));
		}
	}

	public static void main(String []args) throws Exception {
	
		Configuration conf = new Configuration();
		Job job = new Job().getInstance(conf, "bitcoin secondary sort");
		job.setJarByClass(BitcoinSecondarySort.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}


}



