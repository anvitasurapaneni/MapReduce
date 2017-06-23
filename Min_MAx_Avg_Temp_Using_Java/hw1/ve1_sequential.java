package hw1;

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

public class ve1_sequential {
	public static void main(String[] args) throws IOException
	{
		ve1_sequential("/Users/anvitasurapaneni/Downloads/1912.csv");

	}

	static void ve1_sequential(String str){


		System.out.println("version1 sequential without fibonacci");
		// writer writes op to op file
		//PrintWriter writer = new PrintWriter ("file_version1.txt");
		String line;

		// lines used to store raw input lines
		ArrayList<String> lines = new ArrayList<String>();
		try (
				InputStream fis = new FileInputStream(str);
				InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
				BufferedReader br = new BufferedReader(isr);
				) {
			while ((line = br.readLine()) != null) {

				lines.add(line);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// for loop to iterate 10 times to observe 10 readings
		long sumtime  = 0;
		long max = 0;
		long min = 100000;

		for(int i = 0; i < 10; i++){
			// The commom accumilation data structure
			// SumAndCount is a user defined data structure which has attributes sum and count
			// sum stores the sum of the tmax values and count stores count of readings
			HashMap<String,SumAndCount> stationAndTmax=new HashMap<String,SumAndCount>();  
			// initial reading of time
			long time1 = System.currentTimeMillis();
			//iterating over all lines
			for(String l: lines){
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
			sumtime = sumtime + t;
			if(t > max){
				max = t;
			}
			if(t < min){
				min = t;
			}
			//System.out.println("time "+i+" :  "+t);
			//writer.close();
		}

		System.out.println("max:  "+max);
		System.out.println("min:  "+min);
		System.out.println("avg:  "+sumtime/10);



	}
}


