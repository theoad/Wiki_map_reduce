package edu.cmu.scs.cc.project1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper
        extends Mapper<Object, Text, Text, VIntWritable> {

    /**
     * The Mapper class to run the word count job.
     *
     * <p>TODO: Implement the map method
     *
     * <p>Output (word, 1) key/value pair.
     *
     * <p>Hint:
     * StringTokenizer is faster than String.split()
     *
     * @param key input key of the mapper
     * @param value input value of the mapper
     * @param context output key/value pair of the mapper
     */
//  copy paste from the hadoop tutorial:
    private final static VIntWritable one = new VIntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        //  copy paste from the hadoop tutorial:
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}