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
	private int n;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		this.n = context.getConfiguration().getInt("N", 1);
	}

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
			previousElements.add(token);
			if (previousElements.size() == this.n) {
				emitNgrams(context, previousElements);
				previousElements.poll();
			}
		}
		
	}

	private void emitNgrams(Context context, Queue<String> previousElements) throws IOException, InterruptedException {
		Text ngram = new Text(); 

		StringBuilder nGramBuilder = new StringBuilder();

		int count = 0;
		for (String prevElement : previousElements) {
			nGramBuilder.append(prevElement);
			count++;
			if (count < previousElements.size()) {
				nGramBuilder.append(DELIMITER);
			}
		}
		
		ngram.set(nGramBuilder.toString());
		context.write(ngram, ONE);
	}
	
}

