package main;

import persistence_manager.IManager;

public class Client {

	private IManager manager;
	
	public Client(IManager m){
		
		manager = m;
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
	public void Transaction(String[] writes){
		
		
		
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
}
