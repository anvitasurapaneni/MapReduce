package hw5_RbyC;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper.Context;

import hw5_RbyC.PageRank.COUNTER;

import org.apache.hadoop.mapreduce.Reducer;

public class MultiplyMatrix {
	//public static Double dr = 0.0;
}

class MultiplyMapper
// in mapper, // for every node of M matrix, emit its row as the key and column, pagerank contribution as value
// for R ie pagerank matrix is loaded using Distributed cache and pagerank of each Id is stored in a  map
// for evry D or M, we multiply with this R when needed
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
			MatrixTypeIndexVal m = new MatrixTypeIndexVal(type, c, v);
			context.write(new LongWritable(r), m);
		}

		else{
			//for every node of M matrix(Dankling node), emit its Column as the key and row, pagerank contribution as value
			MatrixTypeIndexVal m = new MatrixTypeIndexVal(type, c, v);
			context.write(new LongWritable(r), m);
		}
	}
}

// Undeer reducer, all M items, and D items which need to be multiplied will its corresponding value in R will be
// sent to one reduce job since the key is same.
// matrix multiplications cross product, sum, and pagerank caliculation 
class MultiplyReducer
extends Reducer<LongWritable,  MatrixTypeIndexVal, Text, DoubleWritable> {

	double danglingScore;
	String line;
	HashMap<Long, Double> Rmap;
	long cnt = 1;

	public void setup(Context context) throws IOException{
		danglingScore = 0;
		cnt = context.getConfiguration().getLong("Totalcount", 1);
		// extract the file from local cache, read it and store the pagerank values per id in the hashmap
		Rmap = new HashMap<Long, Double>();
		org.apache.hadoop.fs.Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		System.out.println(localFiles[0].toString());
		File test = new File(localFiles[0].toString());
		BufferedReader br = new BufferedReader(new FileReader(test));
		line = br.readLine();
		while((line = br.readLine()) != null) {
			String[] split = line.split(",");
			String index = split[1];
			long i = Long.parseLong(index);
			long j = Long.parseLong(split[2]);
			double pr = Double.parseDouble(split[3]);
			Rmap.put(i, pr);

		}

	}

	public void reduce(LongWritable key, Iterable<MatrixTypeIndexVal> values,
			Context context
			) throws IOException, InterruptedException {
		// Lists to store M matrix nodes,Dangling nodes(only one value is stored as the value is same through out the row
		List<MatrixTypeIndexVal> Mlist = new ArrayList<MatrixTypeIndexVal>();
		List<MatrixTypeIndexVal> Dlist = new ArrayList<MatrixTypeIndexVal>();

		for (MatrixTypeIndexVal val : values) {
			if(val.type.equals("M")){
				Mlist.add(new MatrixTypeIndexVal(val.type, val.index, val.value));
			}

			else if(val.type.equals("D")){
				Dlist.add(new MatrixTypeIndexVal(val.type, val.index, val.value));
			}

		}


		// Handling dangling nodes, caliculate product of dangling nodes conrtribution and R matrix
		// this value will be added to all pagerank values
		for(MatrixTypeIndexVal d: Dlist){
			if(Rmap.get(d.index) != null)
				danglingScore = danglingScore +  ((d.value) * (Rmap.get(d.index)));
		}

		// cross product for matrix multiplication of M ad R
		// The items with same key will be accumulated by summing up and the 
		//dangling score is added after the page rank has been claiculated.
		double PR = 0;
		for(MatrixTypeIndexVal m: Mlist){
			if(Rmap.get(m.index) != null)
				PR = PR + (m.value * Rmap.get(m.index));

		}
		PR = 0.15/cnt + 0.85*(PR + danglingScore);
		context.write(new Text("R" + "," + key + "," + 1), new DoubleWritable(PR));

	}


}
