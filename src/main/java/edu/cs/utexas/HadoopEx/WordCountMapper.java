package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, Text> {

	// Create a counter and initialize with 1
	private Text counter = new Text();
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		
		while (itr.hasMoreTokens()) {
			String[] lineArray = itr.nextToken().split(",");
			try {
				int delay = Integer.parseInt(lineArray[7]);
				int dayOfWeek = Integer.parseInt(lineArray[0]);
				if (delay >= 0 && dayOfWeek >= 1 && dayOfWeek <= 7) {
					word.set(String.valueOf(dayOfWeek));
					counter.set(lineArray[1] + "," + delay);
					context.write(word, counter);
				}
			} catch (NumberFormatException e) {

			}
			
		}
	}
}