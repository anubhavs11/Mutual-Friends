package org.demo;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CommonFriendReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text>{

	@Override
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter r)
			throws IOException {
		Text[] text=new Text[2];
		int index=0;
		
		while(values.hasNext()) {
			text[index]=new Text(values.next());
			index=index+1;
		}
		try {
		System.out.println(key+":"+text[0]+":"+text[1]);
			String[] list1=text[0].toString().split(" ");
			String[] list2=text[1].toString().split(" ");
			
			List<String> list = new LinkedList<String>();
			
			for (String friend1:list1) {
				for(String friend2:list2) {

					if(friend1.equals(friend2)) {
						list.add(friend1);
					}
				}
			}
			StringBuffer sb=new StringBuffer();
			for (int i=0;i<list.size();i++) {
				sb.append(list.get(i));
				if(i!=list.size()-1) {
					sb.append(" ");
				}
			}
			output.collect(key, new Text(sb.toString()));
		}
		catch(Exception e) {
			
		}
	}
}