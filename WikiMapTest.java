package edu.cmu.scs.cc.project1;

import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class WikiMapTest extends TestCase {

    private Mapper<Object, Text, Text, Text> mapper;
    private MapDriver<Object, Text, Text, Text> driver;

    /**
     * Setup the mapper for word count.
     */
    @Before
    public void setUp() {
        mapper = new WikiMapper();
        driver = new MapDriver<>(mapper);
    }

    @Test
    public void testWikiMapper() throws IOException {
        driver.withMapInputPath(new Path("askdjna-20180308"))
                .withInput(new Text(""), new Text("en Carnegie 100300 2"))
                .withOutput(new Text("Carnegie"), new Text("100300-20180308"))
                .runTest(false);
    }
}
