package hw2;



/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class nocombiner1 {
	
	// Mapper

	public static class NoCombinerMapper
	extends Mapper<Object, Text, Text, TypeAndtemperature1> {


		private Text stID = new Text();
		
		//map: Iterates over each line from input file and processes it.
		//creates object of type TypeAndtemperature1 which stores Type ie TMAX or TMIN and its temperature
		//writes it with the station ID as the key and the object as its value this will be input to reducer

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

//			System.out.println(value.toString());
//			System.exit(1);
	//		String[] lines = value.toString().split(System.getProperty("line.separator"));


	//		for (String line: lines) {
				

				String attrs[] = key.toString().split(",");
				String stationID = attrs[0];
				//      		String date = attrs[1];
				String type = attrs[2];
				String val = attrs[3];
				Float val1 = Float.parseFloat(val);

				stID.set(stationID);
				if(type.equals("TMAX") || type.equals("TMIN")){
					TypeAndtemperature1 tt = new TypeAndtemperature1(type, val1);
					context.write(stID, tt);
				}
		//	}
		}

	}
	
	// reducer

	public static class NoCombinerReducer
	extends Reducer<Text,TypeAndtemperature1,Text,Text> {
		private Text mm = new Text();

		// reduce: accumilates the data from mapper and does the average caliculation
		
		
		public void reduce(Text key, Iterable<TypeAndtemperature1> values,
				Context context
				) throws IOException, InterruptedException {
			Float max = (float)0;
			int maxcnt = 0;
			Float min = (float)0;
			int mincnt = 0;
			Float avgmax = (float)0;
			Float avgmin = (float)0;
			for (TypeAndtemperature1 val : values) {
				if(val.type.equals("TMAX")){

					max += val.temp;
					maxcnt++;
				}else
					if(val.type.equals("TMIN")){

						min += val.temp;
						mincnt++;;
					}

			}

			avgmax = max/maxcnt;
			avgmin = min/mincnt;
			mm.set(avgmin + "\t" + avgmax);
			context.write(key, mm);

		}
	}

//main
	public static void main(String[] args) throws Exception {
//		long time1 = System.currentTimeMillis();
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(nocombiner1.class);
		job.setMapperClass(NoCombinerMapper.class);
		//job.setCombinerClass(NoCombinerReducer.class);
		job.setReducerClass(NoCombinerReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TypeAndtemperature1.class);

		job.setNumReduceTasks(2);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
//		long time2 = System.currentTimeMillis();
		
//		System.out.println("Hadoop No combiner");
//		System.out.println(time2-time1);
	}
}


//TypeAndtemperature1: it has 2 attributes
//it has two attributes 1) type: weather it is TMAX or TMIN 2)temp: the temperature value

class TypeAndtemperature1 implements WritableComparable {
	public String type;
	public float temp;

	public TypeAndtemperature1() {
	}

	public TypeAndtemperature1(String type, Float temp) {
		this.type = type;
		this.temp = temp;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(type);
		out.writeFloat(temp);
	}

	public void readFields(DataInput in) throws IOException {
		this.type = in.readUTF();
		this.temp = in.readFloat();
	}

	public int compareTo(TypeAndtemperature1 o) {
		TypeAndtemperature1 th = this;
		// TODO Auto-generated method stub
	if(this.type.compareTo(o.type) > 0){
		return this.type.compareTo(o.type);
	}
	if(this.type.compareTo(o.type) == 0){
		return (int) (this.temp - o.temp);
	}
	return 0;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

}

