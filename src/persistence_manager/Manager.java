package persistence_manager;

import java.util.Random;

public class Manager implements IManager{

	int[] taids 		= new int[500];
	boolean[] free 		= new boolean[500];
	Buffer2 b 			= new Buffer2();
	
	
	static final private Manager manager;
	
	
	static {
		try{
			manager = new Manager();
		}catch(Throwable e){
			throw new RuntimeException(e.getMessage());
		}
	}
	
	static public Manager getInstance(){
		return manager;
	}
	
	private Manager(){
		super();
		
		for(int i = 0; i < free.length; i++){
			free[i]= true;
		}
		
		
	}
	
	
	@Override
	public synchronized int beginTransaction() {
		//Vergebe eindeutige Transaction ID 
		
		
		 Random randomGenerator = new Random();
		 int randomInt = randomGenerator.nextInt(500);
		
		 while(!free[randomInt]){
			 randomInt = randomGenerator.nextInt(500);
		 }
		 
		 
		 return randomInt;
	}

	@Override
	public synchronized void commit(int taid) {
		//The log is produced during the commit 
		b.commit(taid);
	}

	
	/**
	 * writes the given data with the given page ID
	 * on behalf of the given transaction to the buffer. 
	 * If the given page already exists, its content is
	 * replaced completely by the given data.
	 */
	@Override
	public synchronized void write(int taid, int pageid, String data) {
		b.write(taid,pageid, data);
		if(b.size() >= 5 ){
			
			System.out.println("Lege Logeintrag an");
			b.writeToLog(taid);
			
			System.out.println("Write to persistent");
			b.writetoPersistent();
		}
	
	}

}
