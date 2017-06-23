package hw5_CbyR;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import hw5_CbyR.PageRank.COUNTER;

import org.apache.hadoop.mapreduce.Reducer;



public class CreateMatrix {

}
// uses user defined datatype:
//User defined data type which is writable and comparable:
//contains attributes: 
//type :- The type of the URL "N" represents that the URL is the node, "OL" represents that the URL is an outlink to a node
//pos :- tells the position of the URL, if it is N, it stores the unique id assigned to the URL, this will be its position in the graph
//		  if it is OL, it stores the position of its parent i.e. its corresponding N's position.
//OLsize :- It is the size of out links for its parent(Used to caliculate pagerank distribution)
//			 for N it is 0, for OL it is outlink size of its N

class MatrixMapper
extends Mapper<Object, Text, Text, UrlTypePosPRcount> {
	long cnt;
	MultipleOutputs<Object, Text> mos;
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		mos = new MultipleOutputs(context);
		// cnt is the total no of nodes
		cnt = conf.getLong("count", 1);
	}

	// map takes the input url + outlink and gives an out put of URL name, its UrlTypePosPRcount.
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// split input from parser on tab : URL, out links
		String line = value.toString();
		String[] URLandOL = line.split("\t");
		String URL = URLandOL[0].trim();
		// for every new Node encountered emit its name i.e. URL and its UrlTypePosPRcount

		cnt = cnt + 1;
		context.write(new Text(URL), new UrlTypePosPRcount("N", cnt , 0));
		mos.write(NullWritable.get(), new Text(cnt + "\t" + URL+"~"+"URL"), "Mapping");
		// out links to node
		String ols = URLandOL[1].trim().substring(1, URLandOL[1].length()-1);
		String[] OLs = ols.split(",");
		double olSize = OLs.length;
		// if outlinks exist
		if(ols.length() >= 1){
			// for each out link emit its name and UrlTypePosPRcount
			for(String ol: OLs){
				context.write(new Text(ol.trim()), new UrlTypePosPRcount("OL", cnt, OLs.length));
			}
		}


	}

	public void cleanup(Context context) throws IOException, InterruptedException{
		mos.close();
		context.getCounter(COUNTER.count).setValue(cnt);
	}


}


