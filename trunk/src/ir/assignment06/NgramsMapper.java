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



public class NgramsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

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

		Queue<String> previousElements = new LinkedList<String>();
		StringTokenizer tokenizer = new StringTokenizer(text); // TODO: define delimiters
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			emitNgrams(context, previousElements, token);
			previousElements.add(token);
			if (previousElements.size() > n) {
				previousElements.poll();
			}
		}
		
	}

	private void emitNgrams(Context context, Queue<String> previousElements, String token) throws IOException, InterruptedException {
		Text ngram = new Text(); 
		ngram.set(token);
		context.write(ngram, ONE);
		
		StringBuilder prevGramBuilder = new StringBuilder();
		prevGramBuilder.append(token);
		
		for (String prevElement : previousElements) {
			prevGramBuilder.append(DELIMITER);
			prevGramBuilder.append(prevElement);
			ngram.set(prevGramBuilder.toString());
			context.write(ngram, ONE);
		}
	}
}

