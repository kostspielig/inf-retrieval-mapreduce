package ir.assignment06.input;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class NgramRecordReader extends RecordReader<LongWritable, Text> {
	
	public static byte[] START_TAG;
	public static byte[] END_TAG;
	
	static {
		try {
			START_TAG = "<text xml:space=\"preserve\">".getBytes("UTF-8");
			END_TAG = "</text>".getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}		
	}
	
	private LongWritable currentKey;
	private Text currentValue;
	
	private long startPosition;
	private long endPosition;
	private long currentPosition;
	private DataInputStream inputStream = null;
	private DataOutputBuffer buff = new DataOutputBuffer();

	public NgramRecordReader() {
		currentKey = new LongWritable();
		currentValue = new Text();
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

		FileSplit fileSplit = (FileSplit) split;
		startPosition = fileSplit.getStart();
		Path file = fileSplit.getPath();

		FileSystem fs = file.getFileSystem(context.getConfiguration());

		FSDataInputStream fileIn = fs.open(file);
		fileIn.seek(startPosition);
		inputStream = fileIn;
		endPosition = startPosition + split.getLength();

		currentPosition = startPosition;
	}

	@Override
	public void close() throws IOException {
		inputStream.close();
	}

	@Override
	public float getProgress() throws IOException {
		return ((float) (currentPosition - startPosition)) / ((float) (endPosition - startPosition));
	}

	private boolean readInputStreamUntilMatch(byte[] match, boolean endTag) throws IOException {
		int i = 0;
		while (true) {
			int b = inputStream.read();
			currentPosition++;

			// We have reached the end of file
			if (b == -1) {
				return false;
			}

			if (endTag) {
				buff.write(b);
			}

			if (b == match[i]) {
				i++;
				if (i == match.length) {
					return true;
				}
			} else {
				i = 0;
			}
			
			if (!endTag && i == 0 && currentPosition >= endPosition) {
				return false;
			}
		}
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (currentPosition < endPosition) {
			if (readInputStreamUntilMatch(START_TAG, false)) {
				long recordStartPos = currentPosition;
				try {
					if (readInputStreamUntilMatch(END_TAG, true)) {
						currentKey.set(recordStartPos);
						currentValue.set(buff.getData(), 0, buff.getLength() - END_TAG.length);
						return true;
					}
				} finally {
					buff.reset();
				}
			}
		}
		return false;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return currentKey;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return currentValue;
	}

}
