package hw5_CbyR;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper.Context;

import hw5_CbyR.PageRank.COUNTER;

import org.apache.hadoop.mapreduce.Reducer;

public class MultiplyMatrix {
	//public static Double dr = 0.0;
}

class MultiplyMapper
// in mapper, // for every node of M matrix, emit its Column as the key and row, pagerank contribution as value
// for R ie pagerank matrix, emit its row and key and column, oagerank from previous iteration as value
extends Mapper<Object, Text, LongWritable, MatrixTypeIndexVal> {


	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] parts = line.split(",");
		String type = parts[0];
		long r = Long.parseLong(parts[1]);
		long c = Long.parseLong(parts[2]);
		double v = Double.parseDouble(parts[3]);

		if(type.equals("M")){
			// for every node of M matrix, emit its Column as the key and row, pagerank contribution as value
			MatrixTypeIndexVal m = new MatrixTypeIndexVal(type, r, v);
			context.write(new LongWritable(c), m);
		}
		else if(type.equals("R")){
			// for R ie pagerank matrix, emit its row and key and column, oagerank from previous iteration as value
			MatrixTypeIndexVal m = new MatrixTypeIndexVal(type, c, v);
			context.write(new LongWritable(r),  m);
		}
		else{
			//for every node of M matrix(Dankling node), emit its Column as the key and row, pagerank contribution as value
			MatrixTypeIndexVal m = new MatrixTypeIndexVal(type, r, v);
			context.write(new LongWritable(c), m);
		}
	}
}

// Undeer reducer, all M items, and r items which need to be multiplied will be sent to on ereduce job since the eky is same.
// matrix multiplications cross product is done here
class MultiplyReducer
extends Reducer<LongWritable,  MatrixTypeIndexVal, Text, DoubleWritable> {

	double danglingScore;

	public void setup(Context context){
		danglingScore = 0;
	}

	public void reduce(LongWritable key, Iterable<MatrixTypeIndexVal> values,
			Context context
			) throws IOException, InterruptedException {
		// Lists to store M matrix nodes, R matrix nodes, Dangling nodes(only one value is stored as the value is same through out the row
		List<MatrixTypeIndexVal> Mlist = new ArrayList<MatrixTypeIndexVal>();
		List<MatrixTypeIndexVal> Rlist = new ArrayList<MatrixTypeIndexVal>();
		List<MatrixTypeIndexVal> Dlist = new ArrayList<MatrixTypeIndexVal>();

		for (MatrixTypeIndexVal val : values) {
			if(val.type.equals("M")){
				Mlist.add(new MatrixTypeIndexVal(val.type, val.index, val.value));
			}
			else if(val.type.equals("R")){
				Rlist.add(new MatrixTypeIndexVal(val.type, val.index, val.value));
			}
			else if(val.type.equals("D")){
				Dlist.add(new MatrixTypeIndexVal(val.type, val.index, val.value));
			}

		}


		// Handling dangling nodes, caliculate product of dangling nodes conrtribution and R matrix
		// this value will be added to all pagerank values
		for(MatrixTypeIndexVal d: Dlist){
			//count++;
			for(MatrixTypeIndexVal r: Rlist){
				danglingScore = danglingScore +  ((d.value) * (r.value));
			} 
		}

		// cross product for matrix multiplication of M ad R
		// The items with same key will be accumulated in next iteration
		for(MatrixTypeIndexVal m: Mlist){
			for(MatrixTypeIndexVal r: Rlist){
				context.write(new Text(m.index + "," + r.index), new DoubleWritable(m.value * r.value));

			}
		}


	}

	public void cleanup(Context context){
		// put danglng score in global counter
		context.getCounter(COUNTER.DangligScore).increment((long) (danglingScore * 100000));
	}
}
