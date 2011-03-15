package ir.assignment06;

import ir.assignment06.util.NGramCounter;
import ir.assignment06.util.TextUtil;
import ir.assignment06.util.WikiUtil;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class NgramsMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable ONE = new IntWritable(1);
	private static final String DELIMITER = " ";
	private WikiUtil wikiUtil = new WikiUtil();
	private int n=3;
	
	@Override
	public void map(LongWritable key, Text value, final Context context) throws IOException, 
		InterruptedException {
		context.getCounter(NGramCounter.NR_INPUT_RECORDS).increment(1);
		
		String text = wikiUtil.getPlainTextFromWikiMarkup(value.toString()).toLowerCase();
		text = TextUtil.dropNonAlphaNumericCharacters(text);

		String previousElement = null;
		StringTokenizer tokenizer = new StringTokenizer(text); // TODO: define delimiters
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			emitNgrams(context, previousElement, token);
			previousElement = token;
		}
		
	}

	private void emitNgrams(Context context, String previousElement, String token) throws IOException, InterruptedException {
		
		if (previousElement != null){
			StringBuilder twoGramBuilder = new StringBuilder();
			twoGramBuilder.append(previousElement);
			twoGramBuilder.append(DELIMITER);
			twoGramBuilder.append(token);
			
			Text ngram = new Text(); 
			ngram.set(twoGramBuilder.toString());
			context.write(ngram, ONE);
		}
	}
		

}

