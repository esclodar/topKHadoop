package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, Text, Text, Text> {

   public void reduce(Text text, Iterable<Text> values, Context context)
           throws IOException, InterruptedException {
	   
        PriorityQueue<WordAndCount> topThreeForDay = new PriorityQueue<>();

        for (Text value : values) {
            String[] lineArray = value.toString().split(",");
            topThreeForDay.add(new WordAndCount(
                new Text(text.toString() + "," + lineArray[0]), new IntWritable(Integer.parseInt(lineArray[1]))));
            if (topThreeForDay.size() > 3) {
                topThreeForDay.poll();
            }
        }

        for (int i = 0; i < 3; i++) {
            WordAndCount temp = topThreeForDay.poll();
            context.write(temp.getWord(), new Text(temp.getCount().toString()));
        }
   }
}