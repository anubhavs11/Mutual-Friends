package org.demo;
import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CommonFriendMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key,Text values, OutputCollector<Text,Text> output,Reporter r) throws IOException {
		

		StringTokenizer tokenizer=new StringTokenizer(values.toString(),"]");
		
		String line=null;
		int[] tempArray=null;
		
		while(tokenizer.hasMoreTokens()) {
			
			line=tokenizer.nextToken();
			int index=line.indexOf("[");
			String lineArray1=line.substring(0,index);
			String lineArray2=line.substring(index+1,line.length());
			String[] friends=lineArray2.split("}");
			String[] val = new String[friends.length];
			tempArray = new int[2];
			
			int friendend=friends[0].indexOf(",");
			val[0]=friends[0].substring(6,friendend);
			
			for(int i=1;i<friends.length;i++) {
				String temp=friends[i].substring(1, friends[i].length());
					friendend=temp.indexOf(",");
					val[i]=temp.substring(6,friendend);
			}
			
			
			String str = String.join(" ", val);
			
			for(int i=0;i<friends.length;i++) {
				int userend=lineArray1.indexOf(",");
				
				tempArray[0]=Integer.parseInt(lineArray1.substring(6,userend));
				tempArray[1]=Integer.parseInt(val[i]);
				Arrays.sort(tempArray);
				
				output.collect(new Text(Integer.toString(tempArray[0])+" "+Integer.toString(tempArray[1])), new Text(str));
			}
		}
	}
}
