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




public class ve4_finelock_fib {

	// The shared accumilation data structure
	// SumAndCount is a user defined data structure which has attributes sum and count
	// sum stores the sum of the tmax values and count stores count of readings
	public static HashMap<String,SumAndCount1fib> stationAndTmax = new HashMap<String,SumAndCount1fib>();
	public static void main(String[] args) throws Exception {
		ve4_finelock_fib("/Users/anvitasurapaneni/Downloads/1912.csv");

	}

	static void ve4_finelock_fib(String str){
		System.out.println("version4 fine lock with fibonacci");
		//PrintWriter writer = new PrintWriter ("file_version4_dummy.txt");

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

		List<String> al1 = new ArrayList<String>(lines.subList(0, n1));
		List<String> al2 = new ArrayList<String>(lines.subList(n1, n2));
		List<String> al3 = new ArrayList<String>(lines.subList(n2, n3));
		List<String> al4 = new ArrayList<String>(lines.subList(n3, n4));

		long sumtime  = 0;
		long max = 0;
		long min = 100000;

		for(int i = 0; i < 10; i++){

			long time1 = System.currentTimeMillis();

// dividing load equally to all threads
			Thread t1 = new WorkThread4fib(al1);
			Thread t2 = new WorkThread4fib(al2);
			Thread t3 = new WorkThread4fib(al3);
			Thread t4 = new WorkThread4fib(al4);
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



// avg calculation

			for(Entry<String,SumAndCount1fib> m:stationAndTmax.entrySet())
			{
				SumAndCount1fib sandc = m.getValue();
				float res;
				float cnt = sandc.count;
				float sum = sandc.sum;




				res = sum/cnt;
				//System.out.println(m.getKey()+"\t"+res);
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
			//  System.out.println("The time is: " +t);
		}
		System.out.println("max:  "+max);
		System.out.println("min:  "+min);
		System.out.println("avg:  "+sumtime/10);

	}

	
	// function which has fine lock
	public static void putLock(String stationID, float val1) {

		String str = new String("str");
		SumAndCount1fib SumAndCount1fib = ve4_finelock_fib.stationAndTmax.get(stationID);
		if(SumAndCount1fib != null){
			// lock only on the part of the data structure which belongs to that station id
			synchronized(SumAndCount1fib) {

				// 					SumAndCount1fib.setCount(SumAndCount1fib.count + 1); 
				SumAndCount1fib.count += 1;
				// 					SumAndCount1fib.setSum(SumAndCount1fib.sum + val1);
				SumAndCount1fib.sum += val1;
				SumAndCount.fibonacci(17);
				ve4_finelock_fib.stationAndTmax.put(stationID, SumAndCount1fib);
			}
		}

		else{
			//  locks the entire block because this block involves reading and writing the data and the data 
			// might belong to different stations
			synchronized(str) {
				SumAndCount1fib = ve4_finelock_fib.stationAndTmax.get(stationID);

				if(SumAndCount1fib != null){
					// 							SumAndCount1fib.setCount(SumAndCount1fib.count + 1); 
					SumAndCount1fib.count += 1;
					// 		 					SumAndCount1fib.setSum(SumAndCount1fib.sum + val1);
					SumAndCount1fib.sum += val1;
					SumAndCount.fibonacci(17);
					ve4_finelock_fib.stationAndTmax.put(stationID, SumAndCount1fib);


				}
				else{
					SumAndCount1fib sc1 = new SumAndCount1fib(val1, 1);
					SumAndCount.fibonacci(17);
					ve4_finelock_fib.stationAndTmax.put(stationID, sc1);


				}
			}



		}
	}
}

// work thread
class WorkThread4fib extends Thread {
	List<String> weatherlines;
	WorkThread4fib(List<String> al1) {
		weatherlines = al1;
	}
	@Override
	public void run() {

		System.out.println("I am inside the run method");
		for(String l: weatherlines){
			String attrs[] = l.split(",");
			String stationID = attrs[0];
			String type = attrs[2];
			String val = attrs[3];
			float val1 = Float.parseFloat(val);
			if(type.equals("TMAX")){

				ve4_finelock_fib.putLock(stationID, val1);

			}

		}

	}
}


class SumAndCount1fib{
	float sum;
	float count;

	SumAndCount1fib(float s, float c){
		sum = s;
		count = c;
	}




}