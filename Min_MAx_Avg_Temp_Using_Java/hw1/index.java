package hw1;

import java.util.Scanner;

public class index {
	public static void main(String[] args) throws Exception {

		String src = args[0];
		
		int c = 11;
		
		
		
		
		System.out.println("case 1:  VE1 Sequential execution without delay");
		System.out.println("case 2:  VE2 No-Lock multi threading execution without delay");
		System.out.println("case 3:  VE3 Coarse-Lock multi threading  execution without delay");
		System.out.println("case 4:  VE4 Fine-Lock multi threading  execution without delay");
		System.out.println("case 5:  VE5 No-Sharing multi threading  execution without delay");
		
		System.out.println("case 6:  VE1 Sequential execution with delay");
		System.out.println("case 7:  VE2 No-Lock multi threading execution with delay");
		System.out.println("case 8:  VE3 Coarse-Lock multi threading  execution with delay");
		System.out.println("case 9:  VE4 Fine-Lock multi threading  execution with delay");
		System.out.println("case 10: VE5 No-Sharing multi threading  execution with delay");
		
		System.out.println("case 11:  run all versions");
		
		System.out.println("\n \n enter your choice:");
		Scanner sc = new Scanner(System.in);
		String input = sc.nextLine();
		
		c = Integer.parseInt(input);
		
		
		switch (c) {
		  case 1:
			  ve1_sequential.ve1_sequential(src);

		        break;
		  case 2: 
				ve2_nolock.ve2_nolock(src);
		        break;
		  case 3:
			  

				ve3_coarselock.ve3_coarselock(src);
				break;
			  
			  
		  case 4:        
				ve4_finelock.ve4_finelock(src);
		        break;
		
		  case 5:        
			  ve5_nosharing.ve5_nosharing(src);
		        break;
		        
		  case 6:
			  ve1_sequential_fib.ve1_sequential_fib(src);

		        break;
		  case 7: 
				ve2_nolock_fib.ve2_nolock_fib(src);
		        break;
		  case 8:
			  

				ve3_coarselock_fib.ve3_coarselock_fib(src);
				break;
			  
			  
		  case 9:        
				ve4_finelock_fib.ve4_finelock_fib(src);
		        break;
		
		  case 10:        
			  ve5_nosharing_fib.ve5_nosharing_fib(src);
		        break;
		        
		  case 11:        
			  ve1_sequential.ve1_sequential(src);

				ve2_nolock.ve2_nolock(src);

				ve3_coarselock.ve3_coarselock(src);

				ve4_finelock.ve4_finelock(src);

				ve5_nosharing.ve5_nosharing(src);

				// with fibb


				ve1_sequential_fib.ve1_sequential_fib(src);

				ve2_nolock_fib.ve2_nolock_fib(src);

				ve3_coarselock_fib.ve3_coarselock_fib(src);

				ve4_finelock_fib.ve4_finelock_fib(src);

				ve5_nosharing_fib.ve5_nosharing_fib(src);
		        break;
		        
		  default:
			  ve1_sequential.ve1_sequential(src);

				ve2_nolock.ve2_nolock(src);

				ve3_coarselock.ve3_coarselock(src);

				ve4_finelock.ve4_finelock(src);

				ve5_nosharing.ve5_nosharing(src);

				// with fibb


				ve1_sequential_fib.ve1_sequential_fib(src);

				ve2_nolock_fib.ve2_nolock_fib(src);

				ve3_coarselock_fib.ve3_coarselock_fib(src);

				ve4_finelock_fib.ve4_finelock_fib(src);

				ve5_nosharing_fib.ve5_nosharing_fib(src);
		        break;
		}
		

		///"/Users/anvitasurapaneni/Downloads/1912.csv"

//		ve1_sequential.ve1_sequential(src);
//
//		ve2_nolock.ve2_nolock(src);
//
//		ve3_coarselock.ve3_coarselock(src);
//
//		ve4_finelock.ve4_finelock(src);
//
//		ve5_nosharing.ve5_nosharing(src);
//
//		// with fibb
//
//
//		ve1_sequential_fib.ve1_sequential_fib(src);
//
//		ve2_nolock_fib.ve2_nolock_fib(src);
//
//		ve3_coarselock_fib.ve3_coarselock_fib(src);
//
//		ve4_finelock_fib.ve4_finelock_fib(src);
//
//		ve5_nosharing_fib.ve5_nosharing_fib(src);




	}
}
