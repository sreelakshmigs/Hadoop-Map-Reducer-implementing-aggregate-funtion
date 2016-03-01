package edu.tamu.isys.ratings;

//package edu.tamu.isys.ratings;



import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try{
		String line = value.toString();
		String[] words = line.split("::");
		String temp=words[2];
		String[] genreArray =temp.split(",");
		for (String item : genreArray ) {
			Text keyText=new Text(item);
			String tempValue=words[1]+"~"+words[6];
			value=new Text(tempValue);
			context.write(keyText, value);
		}
	}catch(Exception e){
		e.printStackTrace();
	}
}
}