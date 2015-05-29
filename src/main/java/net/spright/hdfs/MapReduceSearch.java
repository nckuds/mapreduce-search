package net.spright.hdfs;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
//import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduceSearch {
    public static String keyword;
    public static FileSystem fs;
    
    @SuppressWarnings("empty-statement")
    public static void main(String[] args) throws Exception {
        
        //final int searchThreadCount = 10;
        //final int loadThreadCount = 10;
        //final int pageCount = 100000;
        //Map<String, Integer> resultMap = new HashMap<>();
        keyword = args[0];
        Configuration configuration = new Configuration();
        fs = FileSystem.get(configuration);
        //ExecutorService service = Executors.newFixedThreadPool(
        //    searchThreadCount 
        //);
        //BlockingQueue<Path> pageQueue = new ArrayBlockingQueue(pageCount);
        long startTime = System.currentTimeMillis();
        
        //final FileStatus[] status = fs.listStatus(new Path("hdfs://course/user/course"));
        
        
        Path input = new Path("hdfs://course/user/course");
        System.out.println("total file: " + fs.listStatus(input).length);
        Path output = new Path(args[1]);
        Job job = Job.getInstance(configuration, "keyword search");
        job.setJarByClass(MapReduceSearch.class);
        job.setMapperClass(PageMapper.class);
        job.setCombinerClass(PageReducer.class);
        job.setReducerClass(PageReducer.class);
        job.setMapOutputKeyClass(HtmlPage.class);
        System.out.println(job.getMapOutputKeyClass().toString());
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        clearOutput(configuration, output);
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.exit(0);
    }
    private static void waitSomething(int time) throws InterruptedException {
        TimeUnit.SECONDS.sleep(time);
    }
   private static void clearOutput(Configuration conf, Path path)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
    }
        
    public static class PageMapper extends Mapper<Object, Text, HtmlPage, Text> {
        private HtmlPage page;
        
        public void map(Object key, Text value, Context output) throws IOException, InterruptedException {
            page = getHtmlPage(value);
            
            if (page == null || page.link == null 
                            || page.title == null || page.content == null) {
                        return;
                    }
            
            output.write(page, new Text(page.getTitle().toLowerCase()));
            String[] words = page.getContent().trim().split("\\s++");
            for(String word : words) {
                output.write(page, new Text(word));
            }
        }
    }
    public static class PageReducer extends Reducer<HtmlPage, Text, Text, IntWritable> {
        private final static IntWritable result = new IntWritable();
        
        public void reduce(HtmlPage page, Iterable<Text> words, Context output) throws IOException, InterruptedException {
            int score = 0;
            for(Text word : words) {
                if(word.find(keyword) > 0) {
                    if(word.toString().equals(keyword))
                        score += 5;
                    else
                        score += 1;
                }
            }
            //if(score != 0) {
                result.set(score);
                output.write(new Text(page.getLink()), result);
            //}
        }
    }
    private static FileSystem getFileSystem() throws IOException {
       Configuration configuration = new Configuration();
       FileSystem fs = FileSystem.get(configuration);
       return fs;
    }
       
    private static HtmlPage getHtmlPage(Text file)
            throws IOException {     
        
             
        try (BufferedReader reader = new BufferedReader(
            new StringReader(file.toString()))) {
             
            String link = reader.readLine();
            String title = reader.readLine();
            String line;
        
            StringBuilder content = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                content.append(line);
            }
            
            return new HtmlPage(link, title, content.toString());
        }
        
    }
    
    private static void outputSearchResult(Map<String, Integer> map, 
            long totalTime) {
        
        List<Map.Entry<String, Integer>> list =
            new LinkedList<>( map.entrySet() );
        System.out.println("output:");
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare( Map.Entry<String, Integer> o1, 
                    Map.Entry<String, Integer> o2) {
                return (o2.getValue()).compareTo(o1.getValue() );
            }
        });
       Iterator it = list.iterator();
       System.out.println("Links found: ");
       while(it.hasNext()) {
          Map.Entry entry;  
          entry = (Map.Entry) it.next();
          
          System.out.println(entry.getKey().toString() + " / " 
                  + entry.getValue().toString());
        }               
        System.out.println("Search Time: " + totalTime);
        System.out.println("Match link count: " + list.size());
    }      
  
    private static class HtmlPage implements WritableComparable{
        private String link;
        private String title;
        private String content;
        public HtmlPage()
        {}
        
        public HtmlPage(
            String link,
            String title,
            String content
        ) {
            this.link = link;
            this.title = title;
            this.content = content;
        }
        public String getTitle() {
            return title;
        }
        public String getLink() {
            return link;
        }
        public String getContent() {
            return content;
        }
        public void write(DataOutput out) throws IOException {
            out.writeChars(this.link);
            out.writeChars(this.title);
            out.writeChars(this.content);
        }
        public void readFields(DataInput in) throws IOException {
            this.link = in.readLine();
            this.title = in.readLine();
            this.content = in.readLine();
        }
        public int compareTo(Object o) {
            
            return this.link.compareTo(((HtmlPage)o).link);
        }
    }
}
