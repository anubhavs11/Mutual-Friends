package org.demo;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class commonFriend extends Configured implements Tool{
	
	public int run(String args[]) throws IOException  {
		
		JobConf conf=new JobConf(commonFriend.class);
		
		conf.setJobName("Friend");
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(CommonFriendMapper.class);
		conf.setReducerClass(CommonFriendReducer.class);
		
		FileInputFormat.setInputPaths(conf,new Path(args[0]));
		FileOutputFormat.setOutputPath(conf,new Path(args[1]));
		
		JobClient.runJob(conf);
		return 0;
	}
	public static void main(String arg[]) throws Exception {
		int exitCode=ToolRunner.run(new commonFriend(), arg);
		System.exit(exitCode);
	}
}
