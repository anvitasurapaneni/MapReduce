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




public class ve4_finelock {

	
	// The shared accumilation data structure
	// SumAndCount is a user defined data structure which has attributes sum and count
	// sum stores the sum of the tmax values and count stores count of readings
	public static HashMap<String,SumAndCount1> stationAndTmax = new HashMap<String,SumAndCount1>();
	public static void main(String[] args) throws Exception {
		ve4_finelock("/Users/anvitasurapaneni/Downloads/1912.csv");

	}

	static void ve4_finelock(String str){

		System.out.println("Version 4 finelock without fibonacci");

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
			Thread t1 = new WorkThread4(al1);
			Thread t2 = new WorkThread4(al2);
			Thread t3 = new WorkThread4(al3);
			Thread t4 = new WorkThread4(al4);
			// start and run threads
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

			for(Entry<String,SumAndCount1> m:stationAndTmax.entrySet())
			{
				SumAndCount1 sandc = m.getValue();
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
			//System.out.println("The time is: " +t);
		}
		System.out.println("max:  "+max);
		System.out.println("min:  "+min);
		System.out.println("avg:  "+sumtime/10);


	}



	public static void putLock(String stationID, float val1) {

		String str = new String("str");
		SumAndCount1 SumAndCount1 = ve4_finelock.stationAndTmax.get(stationID);
		if(SumAndCount1 != null){
			// lock only on the part of the data structure which belongs to that station id
			synchronized(SumAndCount1) {

				// 					SumAndCount1.setCount(SumAndCount1.count + 1); 
				SumAndCount1.count += 1;
				// 					SumAndCount1.setSum(SumAndCount1.sum + val1);
				SumAndCount1.sum += val1;
				ve4_finelock.stationAndTmax.put(stationID, SumAndCount1);
			}
		}

		else{
		//  locks the entire block because this block involves reading and writing the data and the data 
					// might belong to different stations
			synchronized(str) {
				SumAndCount1 = ve4_finelock.stationAndTmax.get(stationID);

				if(SumAndCount1 != null){
					// 							SumAndCount1.setCount(SumAndCount1.count + 1); 
					SumAndCount1.count += 1;
					// 		 					SumAndCount1.setSum(SumAndCount1.sum + val1);
					SumAndCount1.sum += val1;

					ve4_finelock.stationAndTmax.put(stationID, SumAndCount1);


				}
				else{
					SumAndCount1 sc1 = new SumAndCount1(val1, 1);

					ve4_finelock.stationAndTmax.put(stationID, sc1);


				}
			}



		}
	}
}

// work thread
class WorkThread4 extends Thread {
	List<String> weatherlines;
	WorkThread4(List<String> al1) {
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

				ve4_finelock.putLock(stationID, val1);

			}

		}

	}
}


class SumAndCount1{
	float sum;
	float count;

	SumAndCount1(float s, float c){
		sum = s;
		count = c;
	}




}