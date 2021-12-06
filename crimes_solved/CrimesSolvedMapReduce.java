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

public class CrimesSolvedMapReduce {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
	
		private final IntWritable zero = new IntWritable(0);
		private final IntWritable one = new IntWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String []words = value.toString().split(",");
			if (words[5].equals("State")) return;
			Text state = new Text(words[5]);
			String crimeSolved = words[10];
			context.write(state, crimeSolved.equals("No") ? zero : one);
		} 
	
	}

	public static class AvgReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
	
		private FloatWritable result = new FloatWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int qtNo = 0;
			int qtYes = 0;
			for (IntWritable val : values) {
				if (val.get()==0) qtNo++;
				else qtYes++;
			}
			result.set( ( (float) qtYes ) / ( qtNo + qtYes ) );
			context.write(key, result);
		}	
	
	}
	
	public static void main(String []args) throws Exception {
	  
		Configuration conf = new Configuration();
		Job job = new Job().getInstance(conf, "crimes solver");
		job.setJarByClass(CrimesSolvedMapReduce.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(AvgReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
	

}


