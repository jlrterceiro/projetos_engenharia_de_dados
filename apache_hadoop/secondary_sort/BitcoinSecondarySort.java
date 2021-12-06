import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class BitcoinSecondarySort {
	
	public static class DatePricePair implements WritableComparable<DatePricePair> {
	
		private Text date = new Text();
		private IntWritable price = new IntWritable();
		
		public Text getDate() { return this.date; }
		public IntWritable getPrice() { return this.price; }
		public void setDate(Text date) { this.date = date; }
		public void setPrice(IntWritable price) { this.price = price; }

		@Override
		public int compareTo(DatePricePair pair) {
			int compareValue = this.date.compareTo(pair.getDate());
			if (compareValue == 0) {
				compareValue = this.price.compareTo(pair.getPrice());
				compareValue *= -1;
			}	
			return compareValue;
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
         		date = new Text(Integer.toString(in.readInt()));
         		price = new IntWritable(in.readInt());
       		}
		
		@Override
		public void write(DataOutput out) throws IOException {
        		out.writeInt(Integer.parseInt(date.toString()));
         		out.writeInt(price.get());
       		}
	
	}

	public static class DatePricePartitioner extends Partitioner<DatePricePair, Text> {
	
		@Override
		public int getPartition(DatePricePair pair, Text text, int numberOfPartitions) {
			return Math.abs(pair.getDate().hashCode()%numberOfPartitions);
		}
		
	}	

	public static class DatePriceGroupingComparator extends WritableComparator {
		
		public DatePriceGroupingComparator() {
			super(DatePricePair.class, true);
		}

		@Override
		public int compare(WritableComparable wc1, WritableComparable wc2) {
			DatePricePair pair = (DatePricePair) wc1;
			DatePricePair pair2 = (DatePricePair) wc2;
			return pair.getDate().compareTo(pair2.getDate());
		}

	}

	
	public static class TokenizerMapper extends Mapper<Object, Text, DatePricePair, IntWritable> {
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {			   
			String []words = value.toString().split(",");
			if (words[0].equals("t")) return;
			Date date = new java.util.Date( Long.parseLong(words[0]));
			int price = (int) Float.parseFloat(words[2]);
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			String dateAsString = df.format(date);
			DatePricePair pair = new DatePricePair();
			pair.setDate(new Text(dateAsString));
			pair.setPrice(new IntWritable(price));
			context.write(pair, new IntWritable(price));
		}
	
	}
	
	public static class IntSumReducer extends Reducer<DatePricePair, IntWritable, Text, Text> {
		
		public void reduce(DatePricePair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			String sum  = "";
			int counter = 0;
			for (IntWritable val : values) {
				sum += Integer.toString(val.get()) + " | ";
				counter++;
				if (counter==5) break;
			}
			context.write(key.getDate(), new Text(sum));
		}

	}

	public static void main(String []args) throws Exception {
	
		Configuration conf = new Configuration();
		Job job = new Job().getInstance(conf, "bitcoin secondary sort");
		job.setJarByClass(BitcoinSecondarySort.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(DatePricePair.class);
		job.setOutputValueClass(IntWritable.class);

		job.setPartitionerClass(DatePricePartitioner.class);
		job.setGroupingComparatorClass(DatePriceGroupingComparator.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}

}

