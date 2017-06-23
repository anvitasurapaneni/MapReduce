package hw1;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class ve2_nolock_fib {
	

	// The commom accumilation data structure
	// SumAndCount is a user defined data structure which has attributes sum and count
	// sum stores the sum of the tmax values and count stores count of readings

	public static HashMap<String,SumAndCount> stationAndTmax = new HashMap<String,SumAndCount>();

	public static void main(String[] args) throws Exception {

		ve2_nolock_fib("/Users/anvitasurapaneni/Downloads/1912.csv");
	}

	static void ve2_nolock_fib(String str){

		System.out.println("version2 no lock with fibonacci");
		//PrintWriter writer = new PrintWriter ("file_version2.txt");

		String line;
		List<String> lines = new ArrayList<String>();

		try (
				InputStream fis = new FileInputStream(str);
				InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
				BufferedReader br = new BufferedReader(isr);
				) {
			while ((line = br.readLine()) != null) {
				lines.add(line);
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		int n1 = lines.size() / 4;
		int n2 = lines.size() / 2;
		int n3 = lines.size() / 4 * 3 ;
		int n4 = lines.size() ;

		long sumtime  = 0;
		long max = 0;
		long min = 100000;

		for(int i = 0; i < 10; i++){

			long time1 = System.currentTimeMillis();
			
			// splitiong the array of lines from raw file equally to divide load equally

			List<String> al1 = new ArrayList<String>(lines.subList(0, n1));
			List<String> al2 = new ArrayList<String>(lines.subList(n1, n2));
			List<String> al3 = new ArrayList<String>(lines.subList(n2, n3));
			List<String> al4 = new ArrayList<String>(lines.subList(n3, n4));


			// creating threads and running them

			Thread t1 = new WorkThread2Fib(al1);
			Thread t2 = new WorkThread2Fib(al2);
			Thread t3 = new WorkThread2Fib(al3);
			Thread t4 = new WorkThread2Fib(al4);

			t1.start();
			t2.start();
			t3.start();
			t4.start();

			try {
				t1.join();
				t2.join();
				t3.join();
				t4.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


			// calculating average  Tmax in the main


			for(Entry<String,SumAndCount> m:stationAndTmax.entrySet())

			{
				SumAndCount sandc = m.getValue();
				float res;
				float cnt = sandc.count;
				float sum = sandc.sum;




				res = sum/cnt;
				//	System.out.println("this:  "+m.getKey()+"\t"+res);
				//writer.println(m.getKey()+"\t"+res);

			} 

			//writer.close();
			long time2 = System.currentTimeMillis();
			long t = time2 - time1;

			sumtime = sumtime + t;
			if(t > max){
				max = t;
			}
			if(t < min){
				min = t;
			}
			// System.out.println("The time  is   : " + t);
		}

		System.out.println("max:  "+max);
		System.out.println("min:  "+min);
		System.out.println("avg:  "+sumtime/10);




	}
}

// work thread class
class WorkThread2Fib extends Thread {
	List<String> weatherlines;

	WorkThread2Fib(List<String> al1) {
		weatherlines = al1;
	}

	@Override
	public void run() {


		for(String l: weatherlines){
			// extracting tmax and station id
			String attrs[] = l.split(",");
			String stationID = attrs[0];
			String date = attrs[1];
			String type = attrs[2];
			String val = attrs[3];
			float val1 = Float.parseFloat(val);
			
			// updating common data structure by all threads
			
			if(type.equals("TMAX")){


				if(ve2_nolock_fib.stationAndTmax.get(stationID) == null){

					SumAndCount sc1 = new SumAndCount(val1, 1);
					SumAndCount.fibonacci(17);
					ve2_nolock_fib.stationAndTmax.put(stationID, sc1);



				}

				else{
					float s = ve2_nolock_fib.stationAndTmax.get(stationID).sum;
					float c = ve2_nolock_fib.stationAndTmax.get(stationID).count;
					s = s + val1;
					c= c + 1;
					SumAndCount sc2 = new SumAndCount(s, c);
					SumAndCount.fibonacci(17);
					ve2_nolock_fib.stationAndTmax.put(stationID, sc2);

				}

			}

		}

	}
}



