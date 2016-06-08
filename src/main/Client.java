package main;

import java.util.Random;

import persistence_manager.IManager;

public class Client extends Thread {

	private IManager manager;
	private int start;
	private int stop;
	
	public Client(IManager m, int s){
		
		manager = m;
		
		start = s;
		stop = start + 9;
		
	}
	/**
	 * Mit dieser Methode kann einem Clienten vorgegeben werden, welche writes 
	 * er zu machen hat. 
	 * Beispiel: 
	 * 
	 * String[] writes = {"10:Test","11:Windows","12:Linux","13:Ubuntu",
	 *					   "14:Test","15:Windows","16:Linux","17:Ubuntu",
	 *					   "18:Test","19:Windows"};
	 *			
	 * registerWrites(writes)
	 * Dabei ist die Nummer vor den Strings die PageID
	 * @param writes
	 */
	public void transaction(String[] writes){
		
		
		
		int taid = manager.beginTransaction();
		
		for(int i = 0; i < writes.length; i++){
			String write = writes[i];
			String w[] = write.split(":");
			int pageid = Integer.parseInt(w[0]);
			String data = w[1];
			manager.write(taid, pageid, data);
		}
		
		manager.commit(taid);
	}
	
	
	
	@Override
	public void run() {		
		while(true){
		//Mache eine Kunstpause
				Random randomGenerator = new Random();
				int miliseconds = randomGenerator.nextInt(1500);
				try {
					Thread.sleep(miliseconds);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
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
		
		transaction(writes);
		
		//clientList.add(c);	
		}
		
		
		
		
	}
}
