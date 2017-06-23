package hw2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Scanner;

public class sequential {
	public static void main(String[] args) throws IOException
	{
		ve1_sequential("/Users/anvitasurapaneni/Downloads/1991.csv");

	}

	static void ve1_sequential(String str) throws FileNotFoundException{


		System.out.println("version1 sequential without fibonacci");
		// writer writes op to op file
		//PrintWriter writer = new PrintWriter ("file_version1.txt");
		String line;

		// lines used to store raw input lines
		//ArrayList<String> lines = new ArrayList<String>();
		
				Scanner scan = new Scanner(new File(str));
				
		

		// for loop to iterate 10 times to observe 10 readings
		
			// The commom accumilation data structure
			// SumAndCount is a user defined data structure which has attributes sum and count
			// sum stores the sum of the tmax values and count stores count of readings
			HashMap<String,SumAndCount> stationAndTmax=new HashMap<String,SumAndCount>();  
			// initial reading of time
			long time1 = System.currentTimeMillis();
			//iterating over all lines
			while(scan.hasNext()){
				String l = scan.nextLine();
			//	System.out.println(l);
				// processing the lines and extracting station id and tmax values.
				String attrs[] = l.split(",");
				String stationID = attrs[0];
				String type = attrs[2];
				String val = attrs[3];
				float val1 = Float.parseFloat(val);
				SumAndCount sc = null;
				if(type.equals("TMAX")){
					SumAndCount cst = stationAndTmax.get(stationID) ;
					if(stationAndTmax.get(stationID) != null){

						float s = cst.sum + val1;
						float c = cst.count + 1;
						stationAndTmax.put(stationID, new SumAndCount(s, c));


					}

					else{


						SumAndCount sc1 = new SumAndCount(val1, 1);
						stationAndTmax.put(stationID, sc1);
					}
				}
			}




			for(Entry<String, SumAndCount> m:stationAndTmax.entrySet())

			{
				SumAndCount al = m.getValue();
				Float res = (float) 0;
				Float cnt = al.count;
				Float sum = al.sum;


				res = sum/cnt;

				//System.out.println(m.getKey()+"\t"+res);
				//writer.println(m.getKey()+"\t"+res);

			} 

			long time2 = System.currentTimeMillis();

			long t = time2 - time1;
			System.out.println(t);
			
		

		



	}
}

class SumAndCount{
	float sum;
	float count;
	
	SumAndCount(float s, float c){
		sum = s;
		count = c;
	}
	
	void setSum(float s){
		this.sum = s;
	}
	
	void setCount(float c){
		this.count = c;
	}
	
// Fibonacci function for crating delay in executions
	public static int fibonacci(int n)  {
	    if(n == 0)
	        return 0;
	    else if(n == 1)
	      return 1;
	   else
	      return fibonacci(n - 1) + fibonacci(n - 2);
	}
}
