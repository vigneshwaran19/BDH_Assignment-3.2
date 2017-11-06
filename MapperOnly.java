import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



//DRIVER CLASS
public class MapperOnly 
{

  

public static void main(String[] args) throws Exception 
{
    
if (args.length != 2) 
{
     
 System.err.println("Usage: <input path> <output path>");
     
 System.exit(-1);
    
}
    
	

//Job Related Configurations
	
Configuration conf = new Configuration();
	
Job job = new Job(conf, "My mapper only job");
	
job.setJarByClass(MapperOnly.class);
 

   
    
    // No reducer
 
job.setNumReduceTasks(0);
 

   
    //Provide paths to pick the input file for the job

FileInputFormat.setInputPaths(job, new Path(args[0]));


    
    
    //Provide paths to pick the output file for the job, and delete it if already present
	
Path outputPath = new Path(args[1]);
	
FileOutputFormat.setOutputPath(job, outputPath);
	
outputPath.getFileSystem(conf).delete(outputPath, true);



    
    //To set the mapper and reducer of this job
    
job.setMapperClass(TelesetMapper.class);


    
    
   
    
    //set the input and output format class
    
job.setInputFormatClass(TextInputFormat.class);
    
job.setOutputFormatClass(TextOutputFormat.class);



    //set up the output key and value classes 	
    
job.setOutputKeyClass(IntWritable.class);
    
job.setOutputValueClass(Text.class);


    
    //execute the job
    
System.exit(job.waitForCompletion(true) ? 0 : 1);
  
}

}
