/*
Author: Phani Teja Kesha
*/


import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.util.List;
import java.util.Arrays;
import java.util.Scanner;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class kMeans {
      public static int counter = 0;
      public static int clusters;
      public static int features;
      public static HashMap<Integer,String> imageData = new HashMap<Integer,String>();

      public static List<String> readFileInList(String fileName)
        {

          List<String> lines = Collections.emptyList();
          try
          {
            lines =
             Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8);
          }

          catch (IOException e)
          {

            // do something
            e.printStackTrace();
          }
          return lines;
        }



  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

private final static IntWritable one = new IntWritable(1);
         public static List<String> readFileInList(String fileName)
           {

             List<String> lines = Collections.emptyList();
             try
             {
               lines =
                Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8);
             }

             catch (IOException e)
             {

               // do something
               e.printStackTrace();
             }
             return lines;
           }


         //Distance between two points given
         public double distanceCalculator(String[] point1, String[] point2){
           double dist = 0;

           for(int i=0; i < features; i++){
             dist += Math.pow(( new Double(point1[i]) - new Double(point2[i])), 2);
           }

           return Math.sqrt(dist);
         }


         public Text nearestCentroid(String dataPoint) throws IOException{

           List l = readFileInList("Clusters.txt");
           Iterator<String> itr = l.iterator();
           ArrayList<Double> distances = new ArrayList<>();

           while (itr.hasNext()){
             distances.add(distanceCalculator(itr.next().split(" "), dataPoint.split(" ")));
           }

           int ind = 0;
           int result = 0;
           double minVal = distances.get(0);
           for(int i = 0; i < distances.size(); i++) {
             if (distances.get(i) < minVal) {
               result = i;
               minVal = distances.get(i);
             }
           }
           return new Text(result + "");
         }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      counter+=1;
      context.write(nearestCentroid(value.toString()),new IntWritable());
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {

   public void replaceFile(String filename,int val,String replaceVal){
     try
         {
         File file = new File(filename);
         BufferedReader reader = new BufferedReader(new FileReader(file));
         int lineNumber = val;
         String line = "", oldtext = "";
         int indLine = 0;
         while((line = reader.readLine()) != null)
             {
               if (lineNumber == indLine){
                 oldtext +=replaceVal +"\r\n";
               }
               else{
                 oldtext += line + "\r\n";
               }
               indLine+=1;
         }
         reader.close();
         // replace a word in a file
         //String newtext = oldtext.replaceAll("drink", "Love");

         //To replace a line in a file
         String newtext = oldtext;
         FileWriter writer = new FileWriter(filename);
         writer.write(newtext);
         writer.close();
     }
     catch (IOException ioe)
         {
         ioe.printStackTrace();
   }
}


    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

                         List l = readFileInList("Clusters.txt");




                String[] tempArr1 = l.get(Integer.parseInt(key.toString())).toString().split(" ");


                int c1 = 0;
                for (IntWritable val : values) {
                  String[] tempArr2 = imageData.get(val.get()).split(" ");
                  for(int i = 0;i<tempArr1.length;i++){
                    int tempVal = Integer.parseInt(tempArr1[i])+Integer.parseInt(tempArr2[i]);
                    tempArr1[i]=String.valueOf(tempVal);
                  }
                  c1+=1;
                }

            for(int i =0;i<tempArr1.length;i++){
              int tempVal = Integer.parseInt(tempArr1[i])/c1;
              tempArr1[i]=String.valueOf(tempVal);
              //System.out.println(tempArr1[i]);
            }


        System.out.println("Clusters.txt"+key.toString());
        replaceFile("Clusters.txt",Integer.parseInt(key.toString()),String.join(" ", tempArr1));



      //
      //
      int sum = 0;
       for (IntWritable val : values) {
         sum += val.get();
         //System.out.println(imageData.get(val.get()));
       }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    //System.out.printf("Enter the Clusters: ");
  //  Scanner sc = new Scanner(System.in);
    clusters = Integer.parseInt(args[0]);
    //System.out.printf("Enter No.of Features: ");
    features = Integer.parseInt(args[1]);

    final String FNAME = "Clusters.txt";
    ArrayList<String> list_copy = new ArrayList<>();

    for(int clus=1; clus <= clusters; clus++){
      StringBuilder s1 = new StringBuilder("");
        for(int i=0; i < features; i++) {
          s1.append(clus+" ");
         }
      list_copy.add(s1.toString());
    }

    try ( BufferedWriter bw =
        new BufferedWriter (new FileWriter (FNAME)) )
    {
      for (String line : list_copy) {
        bw.write (line + "\n");
      }

      bw.close ();

    } catch (IOException e) {
      e.printStackTrace ();
    }



    List l = readFileInList("mnist_data.txt");
    Iterator<String> itr = l.iterator();
    int c = 0;
    while (itr.hasNext()){
      imageData.put(c,itr.next());
      c+=1;
    }

    for(int i=0;i<10;i++){
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "kMeans Clustering");
    job.setJarByClass(kMeans.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[2]));
    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  }
}
