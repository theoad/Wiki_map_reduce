package edu.cmu.scs.cc.project1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class WikiReduceTest {
    private Reducer<Text, Text, Text, Text> reducer;
    private ReduceDriver<Text, Text, Text, Text> driver;

    /**
     * Setup the reducer for word count.
     */
    @Before
    public void setUp() {
        reducer = new WikiReducer();
        driver = new ReduceDriver<>(reducer);
    }
    @Test
    public void testWikiReducer() throws IOException {
        String res = "Carnegie";
        for ( int i = 0; i < 30 ; i++) {
            if (i == 0) {
                res.concat("\t" + "150000");
            }
            else if (i == 2) {
                res.concat("\t" + "100000");
            }
            else {
                res.concat("\t" + "0");
            }
        }
        driver.withInput(new Text("Carnegie"), Arrays.asList(new Text("100000-20180308"),
                new Text("100000-20180310"), new Text("50000-20180308")))
                .withOutput(new Text("250000"), new Text(res))
                .runTest(false);
    }
}
