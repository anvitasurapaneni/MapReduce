package hw5_RbyC;

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

import hw5_RbyC.PageRank.COUNTER;

import org.apache.hadoop.mapreduce.Reducer;



public class CreateMatrix1 {


}
// uses user defined datatype:
//User defined data type which is writable and comparable:
//contains attributes: 
//type :- The type of the URL "N" represents that the URL is the node, "OL" represents that the URL is an outlink to a node
//pos :- tells the position of the URL, if it is N, it stores the unique id assigned to the URL, this will be its position in the graph
//		  if it is OL, it stores the position of its parent i.e. its corresponding N's position.
//OLsize :- It is the size of out links for its parent(Used to caliculate pagerank distribution)
//			 for N it is 0, for OL it is outlink size of its N

class MatrixMapperR
extends Mapper<Object, Text, Text, UrlTypePosPRcount> {


	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

	}

	// map takes the input from previous job and gives an out put of URL name, its UrlTypePosPRcount.
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String input  = value.toString();
		String[] KV = input.split("\t");
		String k = KV[0];
		String v = KV[1];
		String[] vs = v.split(",");
		//		System.out.println(vs.length);
		//		System.out.println(v);
		if(vs.length > 2 && (! input.contains("URL"))){
			String type = vs[0];
			long pos = Long.parseLong(vs[1]);
			double OLsize = Double.parseDouble(vs[2]);

			context.write(new Text(k), new UrlTypePosPRcount(type, pos, OLsize));
		}

	}




}


// reducer takes all the UrlTypePosPRcount for a given URL and gives its cell position in the sparse matrix representation 

//uses User defined data type Matrix_XY_PRcontribution:
//contains attributes: 
//type :- The type of the matrix M- represents matrix/ sparse matrix, R represents pagerqnk per URL, D represents dangling nodes
//x:- represents the x position
//y:- represents the y position
//PRcontribution:- page rank contribution of the cell has value i/ out link size(in this task it still stores the outlink size)
class MatrixReducer
extends Reducer<Text, UrlTypePosPRcount, NullWritable, Matrix_XY_PRcontribution> {
	//long c = 0;
	UrlTypePosPRcount Node;
	LinkedList<UrlTypePosPRcount> OutLinks;
	long TotalNodes;
	long c1;

	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Node = null;
		OutLinks = new LinkedList<UrlTypePosPRcount>();
		c1 = conf.getLong("count", 1);
		//System.out.println("-------------------------------------"+c1);
	}
	// for a given url, find its node in Node and also  store UrlTypePosPRcount if the url is an outlink of another node.
	public void reduce(Text key, Iterable<UrlTypePosPRcount> values, Context context) throws IOException, InterruptedException {
		for(UrlTypePosPRcount value: values){

			if(value.type.equals("N")){
				Node = new UrlTypePosPRcount(value.type, value.pos, value.OLsize);
			}
			else if(value.type.equals("OL")){
				OutLinks.add(new UrlTypePosPRcount(value.type, value.pos, value.OLsize));
			}
		}
		// emit them in the format of Matrix_XY_PRcontribution
		// x, y is interchanged with y,x position as per the problem statement if there is a link from y to x, then add to matrix.
		if(Node != null){
			// add an entry for each node irrespective of if it is a dangling node or not
			long x1  = Node.pos;
			Matrix_XY_PRcontribution m1 = new Matrix_XY_PRcontribution("R", x1, 1, 0);
			context.write(NullWritable.get(), m1);
			if(OutLinks.size() == 0){
				// dangling node
				// an R is already added before
			}
			//create cell entry into M for each out link.
			else{
				for(UrlTypePosPRcount ol : OutLinks){
					long y  = (long) Node.pos;
					long x  = (long) ol.pos;
					double OLsize = ol.OLsize;
					Matrix_XY_PRcontribution m = new Matrix_XY_PRcontribution("M",y, x, OLsize);
					context.write(NullWritable.get(), m);

				}
				Node = null;
				OutLinks = new LinkedList<UrlTypePosPRcount>();
			}

		}
		else{
			// Dangling node(Where it is an outlink to a node but is not a key itelf)
			// create one entry in R for such a dangling node
			// evem if OutLinks has multiple entries, they will have the same URL and one entry peer URL into R is required
			for(UrlTypePosPRcount ol : OutLinks){
				c1 = c1 + 1;
				Matrix_XY_PRcontribution m1 = new Matrix_XY_PRcontribution("R", c1, 1, 0);
				context.write(NullWritable.get(), m1);
				break;
			}

			// generate the graph cell representation for such dangling nodes as they have a parent node.
			for(UrlTypePosPRcount ol : OutLinks){
				// m needs to be added
				Matrix_XY_PRcontribution m2 = new Matrix_XY_PRcontribution("M", c1,ol.pos,  0);
				context.write(NullWritable.get(), m2);

			}

		}
		// reinitialise Node and OutLinks to empty
		Node = null;
		OutLinks = new LinkedList<UrlTypePosPRcount>();
		// incrementing global counter by no of dangling nodes
		context.getCounter(COUNTER.count).setValue(c1);

	}


}
