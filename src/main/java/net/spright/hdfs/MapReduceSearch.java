package net.spright.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
        
        final FileStatus[] status = fs.listStatus(new Path("hdfs://course/user/course"));
        System.out.println("total file: " + status.length);
        
        Path input = new Path("hdfs://course/user/course");
        Path output = new Path(args[1]);
        Job job = Job.getInstance(configuration, "keyword search");
        job.setJarByClass(MapReduceSearch.class);
        job.setMapperClass(PageMapper.class);
        job.setCombinerClass(PageReducer.class);
        job.setReducerClass(PageReducer.class);
        job.setOutputKeyClass(HtmlPage.class);
        job.setOutputValueClass(String.class);
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
        
    public static class PageMapper extends Mapper<Object, Path, HtmlPage, String> {
        
        @Override
        public void map(Object key, Path value, Context context) throws IOException, InterruptedException {
            HtmlPage page = getHtmlPage(fs, value);
            context.write(page, page.title.toLowerCase());
            for(String word : page.content.trim().split("\\s++")) {
                context.write(page, word);
            }
        }
    }
    public static class PageReducer extends Reducer<HtmlPage, String, String, IntWritable> {
        private final IntWritable result = new IntWritable();
        @Override
        public void reduce(HtmlPage page, Iterable<String> words, Context context) throws IOException, InterruptedException {
            int score = 0;
            for(String word : words) {
                if(word.contains(keyword)) {
                    if(page.title.toLowerCase().equals(keyword))
                        score += 5;
                    else
                        score += 1;
                }
            }
            //if(score != 0) {
                result.set(score);
                context.write(page.link, result);
            //}
        }
    }
    private static FileSystem getFileSystem() throws IOException {
       Configuration configuration = new Configuration();
       FileSystem fs = FileSystem.get(configuration);
       return fs;
    }
       
    private static HtmlPage getHtmlPage(FileSystem fs, Path path)
            throws IOException {     
        
        if (!fs.getFileStatus(path).isFile()){
            return null;
        }
                
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(
                fs.open(path)
                , Charset.forName("UTF-8")))) {
             
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
  
    private static class HtmlPage {
        private final String link;
        private final String title;
        private final String content;
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
    }
}
