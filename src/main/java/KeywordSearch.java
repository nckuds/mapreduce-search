import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KeywordSearch {
    public static class TokenizerMapper
         extends Mapper<Text, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private String keyword;
        @Override
        protected void setup(Context context
                       ) throws IOException, InterruptedException {
            keyword = context.getConfiguration().get(KEYWORD);
        }
        @Override
        public void map(Text key, Text value, Context context
                        ) throws IOException, InterruptedException {
            if (keyword == null) {
                return;
            }
            //System.out.println(value.toString());
            //System.out.println("next value.");
            if (value.toString().contains(keyword)) {
                context.write(key, one);
            }
        }
    }
    public static class IntSumReducer
         extends Reducer<Text,IntWritable,Text,IntWritable> {
        private final IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
                           ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
              sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    private static void clearOutput(Configuration conf, Path path)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
    }
    private static FileSystem getFileSystem() throws IOException {
       Configuration configuration = new Configuration();
       FileSystem fs = FileSystem.get(configuration);
       return fs;
    }
    
     private static String getLink(FileSystem fs, Path path)
            throws IOException {     
        
        if (!fs.getFileStatus(path).isFile()){
            return null;
        }
                
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(
                fs.open(path)
                , Charset.forName("UTF-8")))) {
             
            String link = reader.readLine();
      
            
            return link;
        }
        
    }
    
    private static void printResult(String keyword, Path path, long totalTime) throws IOException {
        Map<String, Integer> resultMap = new HashMap<>();
        System.out.println("--------------------");
        //System.out.println("Output: " + path);
        Configuration configuration = new Configuration();
        final FileSystem fs = getFileSystem();
        if (!fs.getFileStatus(path).isFile()){
            return;
        }
                
          try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(
                fs.open(path)
                , Charset.forName("UTF-8")))) {

            String line;
        
            StringBuilder content = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                 
                 String key;
                 int value;
                 String [] sArray;
                 sArray = line.split(":::///");
                 key = sArray[0];
                 value = Integer.valueOf(sArray[1]);
                 if(keyword.contains(key)) {
                     value += 5;
                 };
                 
                 resultMap.put(getLink(fs, new Path(key)), value);
            }
        }
        outputSearchResult(resultMap, totalTime);
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
    
      
    
    private static final String KEYWORD = "course.keyword";
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(FileInputFormat.INPUT_DIR_RECURSIVE,
                String.valueOf(true));
        conf.set("mapred.textoutputformat.separator", ":::///");
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        conf.set(KEYWORD, args[2]);
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(KeywordSearch.class);
        job.setInputFormatClass(TextInputFormatV2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        clearOutput(conf, output);
        long startTime = System.currentTimeMillis();
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        job.waitForCompletion(true);
        long endTime   = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        printResult(args[2], new Path("hdfs://course/user/course/" + args[1] + "/part-r-00000" ), totalTime);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
