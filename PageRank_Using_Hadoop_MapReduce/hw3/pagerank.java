package HW3New;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

 class BooleanFloat{
	boolean boo;
	float fl;
	BooleanFloat(boolean b, float f){
		this.boo = b;
		this.fl = f;
		
	}
}


public class pagerank {
	
	static long noOfPages;
	
	 static enum COUNTERS {
		 DanglingTotalPR, NodesCount;
		 }
	
	static final int K = 5;
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
	
		// store number of pages and store it in PagesCount for each job
		noOfPages = ParseTempInput(conf, args[0]);
		// store number of pages in PagesCount
		conf.setLong("PagesCount", noOfPages);
		
		
		readInput(conf);
		// store number of pages in PagesCount
		conf.setLong("PagesCount", noOfPages);
		
		int ii = 0;
		while (ii < 9) {
			
			float bf = iterate(conf, ii++);
			conf.setFloat("dw", bf);
			// store number of pages in PagesCount
			conf.setLong("PagesCount", noOfPages);
		}
		
		writeOutput(conf, ii, args[1]);
	
	}
	
// parses the input data and emits the nodename followed by its list of outlinks 
	// this will be written to a file which will be input to readinput job.
	public static Long ParseTempInput(Configuration conf, String otherArgs) throws Exception {
		Job job = Job.getInstance(conf, "Parse input");
		job.setJarByClass(pagerank.class);
		job.setMapperClass(ParseMapper.class);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem fs = FileSystem.get(conf);
   
	    FileInputFormat.addInputPath(job, new Path(otherArgs));
	    FileOutputFormat.setOutputPath(job, new Path("pgrkParse"));

		 job.waitForCompletion(true);
		 long NoOfpages = job.getCounters().findCounter(COUNTERS.NodesCount).getValue();
		 

		 return NoOfpages;
		
	}
	
	
// readinput is the initial mapper
	// makes the first emit of text, node by processing the data
	// uses InputMapper and PRreducer
	public static void readInput(Configuration conf) throws Exception {
		Job job = Job.getInstance(conf, "read input");
		job.setJarByClass(pagerank.class);
		job.setMapperClass(InputMapper.class);
		job.setReducerClass(PRreducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Node.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileSystem fs = FileSystem.get(conf);
        
		FileInputFormat.addInputPath(job, new Path("pgrkParse"));
		FileOutputFormat.setOutputPath(job, new Path("pgrk0"));

		 job.waitForCompletion(true);
		

		
		
	}

	// this is the itr=erative task that runs 10 times
	// calls prMapper and prReducer
	public static float iterate(Configuration conf, int ii) throws Exception {
		Job job = Job.getInstance(conf, "page rank iteration");
		job.setJarByClass(pagerank.class);
		job.setMapperClass(PRmapper.class);
		job.setReducerClass(PRreducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Node.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path("pgrk" + (ii + 1)), true);
        
		FileInputFormat.addInputPath(job, new Path("pgrk" + ii));
		FileOutputFormat.setOutputPath(job, new Path("pgrk" + (ii + 1)));

		 job.waitForCompletion(true);
		 

		

		long dw = job.getCounters().findCounter(COUNTERS.DanglingTotalPR).getValue();
		float dw1 = ((float) dw);

		// the file pgrk10 will be input for writeoutput job
		if(ii != 9){
				fs.delete(new Path("pgrk" + ii), true);
		}
		
		

		return dw1/ 1000000000 / 10000000;
	}
	// write output uses sort reducer to sort the pages based on pagerank value and prints top 10
	public static void writeOutput(Configuration conf, int ii, String oppath) throws Exception {
	conf.setInt("itr", ii);
		
		Job job = Job.getInstance(conf, "page rank iteration");
		job.setJarByClass(pagerank.class);
		job.setMapperClass(Mapper.class);
		job.setPartitionerClass(SortPartitioner.class);
		job.setReducerClass(SortReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Node.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(10);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path("pgrk9"));
		FileOutputFormat.setOutputPath(job, new Path(oppath));

		job.waitForCompletion(true);
		
	}

}

class Node implements Writable {

	String RootNodeName;
	public  String Name;
	public float pr;
	public ArrayList<String> links;
	public  boolean IsDangling;

	public Node() {
		this.RootNodeName = "";
		this.Name = "";
		this.pr = (float)0.0;
		this.links = new ArrayList<String>();
		IsDangling = false;
	}

	public Node(String NodeName, float pr, ArrayList<String> links) {
		
		this.RootNodeName = NodeName;
		this.pr = pr;
		this.links = links;
	}

	public void write(DataOutput out) throws IOException {

		out.writeUTF(RootNodeName);
		
		out.writeUTF(Name);
		
		out.writeFloat(pr);
		
		out.writeInt(links.size());
		
		for (String dst : links) {
			out.writeUTF(dst);
		}
		out.writeBoolean(IsDangling);
	}

	public void addToList(String str){
		links.add(str);
	}
	
	public void readFields(DataInput in) throws IOException {

		this.RootNodeName = in.readUTF();
		
		this.Name = in.readUTF();
		
		this.pr = in.readFloat();
		
		int nn = in.readInt();
		links = new ArrayList<String>(nn);
		for (int ii = 0; ii < nn; ++ii) {
			links.add(in.readUTF());
		}
		
		this.IsDangling = in.readBoolean();
	}
	
	public String toString(){
		return " "+pr;
	}

	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}


}
