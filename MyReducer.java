package edu.tamu.isys.ratings;

//package edu.tamu.isys.ratings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		try{
			HashMap<Text, Double> genreValue = new HashMap<Text, Double>();
			int movieCounter=0;
			List<String> movieNames=new ArrayList<String>();
			List<Double> ratings=new ArrayList<Double>();
			String[] valueSplit;
			for(Text val: values){
				String row = val.toString();
				valueSplit=row.split("~");
				movieNames.add(valueSplit[0]);
				ratings.add(Double.parseDouble(valueSplit[1]));
			}
			Double rating;
			Double sumOfRating;
			Double avgRating;
			for (int i=0;i<movieNames.size();i++){
				sumOfRating=0.0;
				movieCounter=0;
				for(int j=0;j<movieNames.size();j++){
					if(movieNames.get(i).equals(movieNames.get(j))&&!genreValue.containsKey(movieNames.get(i))){
						movieCounter++;
						rating=ratings.get(j);
						sumOfRating=sumOfRating+rating;
					}
				}			
				if (!genreValue.containsKey(movieNames.get(i))){
					avgRating=(sumOfRating/movieCounter);
					genreValue.put(new Text(movieNames.get(i)), avgRating);
				}
			}
			Double highestRating = new Double(0);
			Text bestMovie=new Text();
			for(Map.Entry<Text, Double> entry: genreValue.entrySet()){
				if(highestRating<entry.getValue()){
					highestRating=entry.getValue();
					bestMovie=entry.getKey();
				}		
			}
			String finalValue=bestMovie.toString().concat(highestRating.toString());
			Text value= new Text(finalValue);
			if(key!=null){
			context.write(key,value);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
