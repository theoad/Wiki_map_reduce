package edu.cmu.scs.cc.project1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer
        extends Reducer<Text, VIntWritable, Text, VIntWritable> {

    /**
     * The Reducer class to run the word count job.
     *
     * <p>TODO: Implement the reducer for word count.
     *
     * <p>Output (word, count) key/value pair.
     *
     * @param key input key of the reducer
     * @param values input value of the reducer
     * @param context output key/value pair of the reducer
     */
    //  copy paste from the hadoop tutorial:
    private VIntWritable result = new VIntWritable();
    public void reduce(Text key, Iterable<VIntWritable> values, Context context)
            throws IOException, InterruptedException {
        //  copy paste from the hadoop tutorial:
        int sum = 0;
        for (VIntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}