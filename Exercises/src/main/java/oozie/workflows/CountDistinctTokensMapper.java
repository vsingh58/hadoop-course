package oozie.workflows;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountDistinctTokensMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable countOne = new IntWritable(1);
	private final Text text = new Text();
	@Override	
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		while (tokenizer.hasMoreTokens()) {
			text.set(tokenizer.nextToken());
			context.write(text, countOne);
		}
	}
}