package hw5_RbyC;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class CaiculatePageRankContribution {


}

class PRcontrMapper
extends Mapper<Object, Text, NullWritable, Matrix_XY_PRcontribution> {
	// to print the sparse matrix for matrix M. It is reused as it is. Its value does not change.
	//PrintWriter pw;
	MultipleOutputs<Object, Text> mos;
	long c1;

	public void setup(Context context) throws FileNotFoundException{
		Configuration conf = context.getConfiguration();
		mos = new MultipleOutputs(context);
		// c1 = count of all non-dangling nodes from previous job
		c1 = conf.getLong("count", 1);

	}

	//uses User defined data type Matrix_XY_PRcontribution:
	//contains attributes: 
	//type :- The type of the matrix M- represents matrix/ sparse matrix, R represents pagerqnk per URL, D represents dangling nodes
	//x:- represents the x position
	//y:- represents the y position
	//PRcontribution:- page rank contribution of the cell has value i/ out link size(in this task it still stores the outlink size)

	// write to either R(R is cached and used in pagerank caliculation) or MandD based on type
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// extract type x, y, Out link size from previous jobs output
		String matrix[] = value.toString().split(",");
		String type = matrix[0];
		long x = Long.parseLong(matrix[1].trim());
		long y = Long.parseLong(matrix[2].trim());
		double Ol_size = Double.parseDouble(matrix[3].trim());

		if(type.equals("M") && Ol_size == 0){
			// dangling node if M's outlink size is zero i.e. url has no outlinks
			//c or c1 is value is the total of links in graph
			// Dangling graph is a one dimentional graph because its value will be same along all cells in a row, hence only one cell maintained 
			long c = c1;
			Matrix_XY_PRcontribution m = new Matrix_XY_PRcontribution("D", 1, y, 1.0/c);
			mos.write("MandD", NullWritable.get(), m);

		}
		else if(type.equals("R")){
			//when it is pagerank, initial pagerank is stored as pagerank distribution i.e. value is 1/ total no of nodes.
			long c = c1;
			Matrix_XY_PRcontribution m = new Matrix_XY_PRcontribution(type, x, y, 1.0/c);
			mos.write("R", NullWritable.get(), m);
		}
		else{
			// for other cells of matrix M, the pagerank distribution will be 1/ ol size of its parent.
			Matrix_XY_PRcontribution m = new Matrix_XY_PRcontribution(type, x, y, 1.0/Ol_size);
			mos.write("MandD", NullWritable.get(), m);
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException{
		mos.close();

	}
}