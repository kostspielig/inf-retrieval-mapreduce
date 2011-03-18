package ir.assignment06;

import ir.assignment06.util.TextUtil;
import ir.assignment06.util.WikiUtil;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class NgramsMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable ONE = new IntWritable(1);
	private WikiUtil wikiUtil = new WikiUtil();
	
	@Override
	public void map(LongWritable key, Text value, final Context context) throws IOException, 
		InterruptedException {
		
		String text = wikiUtil.getPlainTextFromWikiMarkup(value.toString()).toLowerCase();
		text = TextUtil.dropNonAlphaNumericCharacters(text);
		StringTokenizer tokenizer = new StringTokenizer(text); // TODO: define delimiters
		while (tokenizer.hasMoreTokens()) {
			Text ngram = new Text(); 
			ngram.set(tokenizer.nextToken());
			context.write(ngram, ONE);
		}
		
	}
}

