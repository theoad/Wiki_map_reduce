package edu.cmu.scs.cc.project1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer utility.
 */
public class WikiReducer
        extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // Implement the code here for reducer of wiki data analysis,
        // you may need to change the key/value pair format as per your design
        int[] dates_array = new int[30]; //30 days
        for (Text val : values) {
            try {

                String[] split_val = val.toString().split("-");
                int views = Integer.parseInt(split_val[0]);
                String date = split_val[1];
                //the date format is YYYYMMDD
                int month  = Integer.parseInt(date.substring(4,6)); //MM
                int day = Integer.parseInt(date.substring(6,8)); //DD

                if(month == 3) {
                    int offset = 8; //the record are from march 8 to april 6
                    dates_array[day-offset] += views;
                }
                else if (month == 4) {
                    int offset = 23;
                    dates_array[day + offset] += views;
                }
            }
            catch (Exception exception) {
                //ignore
            }
        }
        int sum = 0;
        for (int val : dates_array) {
            sum+=val;
        }
        if (sum > 100000) {
            String res_temp = key.toString();
            for (int val : dates_array) {
                res_temp.concat("\t"+Integer.toString(val));
            }
            result.set(res_temp);
            context.write(new Text(Integer.toString(sum)),result);
        }
    }
}
