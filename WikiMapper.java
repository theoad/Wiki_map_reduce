package edu.cmu.scs.cc.project1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Mapper Utility.
 */
public class WikiMapper
        extends Mapper<Object, Text, Text, Text> {

    private Text title = new Text();
    private Text date_and_view_num = new Text();
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        // Implement the code here for mapper of wiki data analysis,
        // you may need to change the key/value pair format as per your design
        String[] decoded_line = DataFilter.getColumns(value.toString());

        if(DataFilter.checkAllRules(decoded_line)) {
            //get the file name
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();

            //get the date of the data
            String[] splited_filename = filename.split("-");

            try {
                title.set(decoded_line[1]);
                date_and_view_num.set(decoded_line[2] + "-" + splited_filename[1]);

                context.write(title, date_and_view_num);
            }
            catch (Exception exception) {
                //ignore if the file name is not legal
            }
        }
    }
}
