package main;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import persistence_manager.IManager;
import persistence_manager.Manager;

public class startup {

	static IManager m;
	static List<Client> clientList;
	static int start,stop;
	
	public static void main(String[] argsv) {
		
		m = new Manager();
		clientList = new ArrayList();
		
		int i = 0; 
		while (i < 20){
			start = 10 + (i* 10) ;
			stop  = start + 9;
			
			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {		
					Client c = new Client(m);
					int pagecounter = 0;
					String  S 	= "Kolja:Test:Windows:Mac:Ubuntu:Linux:Biber:Wacken:9:10";
					String[] s	= S.split(":");
					String[] writes = new String[10];
					int x = 0;
					for(int u = start; u < stop+1 ; u++){
						String write =u+":"+s[x];
						writes[x] = write;
						x++;
					}
					c.Transaction(writes);
					clientList.add(c);	
				}
		});
	
		
		//Mache eine Kunstpause
		Random randomGenerator = new Random();
		int miliseconds = randomGenerator.nextInt(1500);
		try {
			Thread.sleep(miliseconds);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		t.run();
		i++;
		
		
	}
}
}
