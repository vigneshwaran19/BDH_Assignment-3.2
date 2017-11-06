import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

import java.util.*;


//MAPPER CLASS




public class TelesetMapper extends Mapper <LongWritable, Text, NullWritable, Text>


{

public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException


{


final Text word = new Text();

NullWritable a= NullWritable.get();

String line=value.toString();

StringTokenizer iterator = new StringTokenizer(line,"\\|");

String company = iterator.nextToken();

String product = iterator.nextToken();

if(company.matches("NA") | product.matches("NA"))

{
	
	
}

else
{
	
word.set(line);

context.write(a,word);

}

}

}
