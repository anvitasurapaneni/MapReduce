package hw5_CbyR;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class PageRank {
	// global counters
	static enum COUNTER {count, DangligScore};
	//static double dang;
	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();

		ParseInput(conf, args[0]);
		long cnt = CreateMatrixM(conf);
		long cntTotal = CreateMatrixR(conf, cnt);
		CaliculatePRcontr(conf, cntTotal);
		//first iteration
		double dang = MultiplyMatrix(conf, 1, "matrixMultiplyOutput123");
		CaliculatePagerank(conf, cntTotal, dang);
		int i = 2;
		//iteration 2 and above
		while(i<=10){
			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path("matrixMultiplyOutput123"), true);
			dang = MultiplyMatrix(conf, i, "matrixSumMultiplyOutput123");
			fs.delete(new Path("matrixSumMultiplyOutput123"), true);
			CaliculatePagerank(conf, cntTotal, dang);
			i++;
		}
		Top100(conf);
		FinalOutput(conf, args[1]);

	}

	// ParseTempInput parses the given input file and emits the nodename as key followed by its outlinks as value
	public static void ParseInput(Configuration conf, String Args0) throws Exception {  

		Job job = new Job(conf, "parse");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(ParseMapper.class);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(Args0));
		FileOutputFormat.setOutputPath(job,new Path("parseOutput"));
		job.waitForCompletion(true);
	}

	// takes the parsed data as input and generates sparse matrix form map job
	// gives count of all non-dangling nodes
	public static long CreateMatrixM(Configuration conf) throws Exception {  

		conf.setLong("count", 0);
		Job job = new Job(conf, "ceate matrix");

		job.setJarByClass(PageRank.class);
		job.setMapperClass(MatrixMapper.class);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(UrlTypePosPRcount.class);
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path("parseOutput"));
		FileOutputFormat.setOutputPath(job,new Path("createMatrixOutputM"));
		MultipleOutputs.addNamedOutput(job, "Mapping", TextOutputFormat.class, NullWritable.class, Text.class);
		job.waitForCompletion(true);
		return job.getCounters().findCounter(COUNTER.count).getValue();
	}

	// takes the parsed data as input and generates sparse matrix form reduce job
	// gives count of all  nodes including dangling, passed as input to Handle dangling 
	public static long CreateMatrixR(Configuration conf, long cnt) throws Exception {  
		System.out.println("count in sending"+cnt);
		conf.setLong("count", cnt);
		Job job = new Job(conf, "ceate matrix");

		job.setJarByClass(PageRank.class);
		job.setMapperClass(MatrixMapperR.class);
		job.setReducerClass(MatrixReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(UrlTypePosPRcount.class);
		job.setOutputKeyClass(NullWritable.class);
		//job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputValueClass(Matrix_XY_PRcontribution.class);
		FileInputFormat.addInputPath(job, new Path("createMatrixOutputM"));
		FileOutputFormat.setOutputPath(job,new Path("createMatrixOutput"));
		job.waitForCompletion(true);
		return job.getCounters().findCounter(COUNTER.count).getValue();
	}

	// handles dangling nodes of the previous graph and also assign each cell's page rank contribution
	// cnt is total no of nodes
	public static void CaliculatePRcontr(Configuration conf, long cnt) throws Exception {  

		// cntTotal
		conf.setLong("count", cnt);
		Job job = new Job(conf, "ceate matrix");

		job.setJarByClass(PageRank.class);
		job.setMapperClass(PRcontrMapper.class);
		job.setReducerClass(Reducer.class);
		//		job.setMapOutputKeyClass(Text.class);
		//		job.setMapOutputValueClass(UrlTypeAndPosition.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Matrix_XY_PRcontribution.class);
		FileInputFormat.addInputPath(job, new Path("createMatrixOutput/part-r-00000"));
		FileOutputFormat.setOutputPath(job,new Path("createDangMatrixDangOutput"));
		MultipleOutputs.addNamedOutput(job, "M", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "MandR", TextOutputFormat.class, NullWritable.class, Text.class);




		job.waitForCompletion(true);
	}

	// creates cross product for multiplying Ma nd R
	public static double MultiplyMatrix(Configuration conf, int itr, String output) throws Exception {  

		Job job = new Job(conf, "create matrix");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(MultiplyMapper.class);
		job.setReducerClass(MultiplyReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(MatrixTypeIndexVal.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		// for first iteration, input will be output of previous job which contains both R and M
		if(itr == 1){
			FileInputFormat.addInputPath(job, new Path("createDangMatrixDangOutput/MandR-m-00000"));
			// for the corresponding iterations, we pass The M matrix which is stored to a file(Does not change)
			// and R from another fie which will be the output of CaliculatePagerank of the previos iteration
		}else{
			MultipleInputs.addInputPath(job, new Path("createDangMatrixDangOutput/M-m-00000"), TextInputFormat.class, MultiplyMapper.class);
			MultipleInputs.addInputPath(job, new Path(output), TextInputFormat.class, MultiplyMapper.class);
		}

		FileOutputFormat.setOutputPath(job,new Path("matrixMultiplyOutput123"));
		job.waitForCompletion(true);
		long dang = job.getCounters().findCounter(COUNTER.count).getValue();
		double d = dang/100000;
		return d;
	}

	// sums up the crossproduct values belonging to one cell, adds the dangling score and caliculates pagerank.
	// danglingScore is the dangling contribution for each page
	public static void CaliculatePagerank(Configuration conf, long cnt, double danglingScore) throws Exception {  
		conf.setFloat("danglingScore", (float)danglingScore);
		conf.setLong("count", cnt);
		Job job = new Job(conf, "create matrix");


		job.setJarByClass(PageRank.class);
		job.setMapperClass(CaliculatePagerankMapper.class);
		job.setReducerClass(CaliculatePagerankReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path("matrixMultiplyOutput123"));
		FileOutputFormat.setOutputPath(job,new Path("matrixSumMultiplyOutput123"));
		job.waitForCompletion(true);
	}

	// gives the top 100 pages with the pageID and page rank value
	public static void Top100(Configuration conf) throws Exception {  

		Job job = new Job(conf, "ceate matrix");
		job.setJarByClass(PageRank.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setPartitionerClass(PagePartitioner.class);
		job.setMapperClass(Top100Mapper.class);
		job.setReducerClass(Top100Reducer.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("matrixSumMultiplyOutput123"));
		FileOutputFormat.setOutputPath(job,new Path("Top100"));
		job.waitForCompletion(true);
	}

	// maps the page ID with URL name and gives top 100 URLs with paerank in sorted order
	public static void FinalOutput(Configuration conf, String output) throws Exception {  

		Job job = new Job(conf, "ceate matrix");
		job.setJarByClass(PageRank.class);
		job.setPartitionerClass(PagePartitioner.class);
		job.setMapperClass(GetURLMapper.class);
		job.setReducerClass(GetURLReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		// this has multiple inputs one from the result of top 100 where id to pagerank mapping is tehre
		MultipleInputs.addInputPath(job, new Path("Top100/part-r-00000"), TextInputFormat.class, GetURLMapper.class);
		// another file is the id to full name mapping file
		MultipleInputs.addInputPath(job, new Path("createMatrixOutputM/Mapping-m-00000"), TextInputFormat.class, GetURLMapper.class);
		FileOutputFormat.setOutputPath(job,new Path(output));
		job.waitForCompletion(true);
	}

}


