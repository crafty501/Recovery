package main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import persistence_manager.Page;

/**
 * Due to the noforce, nosteal, non-atomic implementation of the persistence manager, no undo recovery
 * is required after a system failure, but a redo recovery is.
 * @author callya
 *
 */
public class Recovery {

	
	static String filename;
	static String prefix = "Memory";
	
	private static int readLSNFromPage(String Filename){
		try{
		BufferedReader br = new BufferedReader( new FileReader(prefix+"/"+Filename));
		String line = br.readLine();
		String elements[] = line.split(",");
		return Integer.parseInt(elements[1]);
		}catch(IOException e){
			System.out.println(e.getMessage());
			return -1;
		}
			
		}
	/**
	 * Diese Methode geht die Log-File zeilenweise durch und vergleicht dabei die 
	 * lsn des Log Eintrags mit der lsn der entsprechenden Page im persistenten Speicher
	 * Ist die LSN im Speicher kleiner als die in der Log-File müssen wir ein redo machen.
	 * 
	 */
	private static void recover(){
		try{
			filename ="Log";
			int recoveryCount = 0;
			BufferedReader br = new BufferedReader( new FileReader(filename));
			String line = br.readLine();
			while(line != null){
				if(!line.equals(" ")){
				String elements[] = line.split(",");
				//erstelle Page von der Log-File
				//[taid,pid,lsn,data]
				int taid 	    = Integer.parseInt(elements[0]); 
				int pid 		= Integer.parseInt(elements[1]);
				int log_lsn 	= Integer.parseInt(elements[2]);
				String data 	= elements[3];
				//Wenn in der logfile eine neuere lsn gefunden wird, als im persistenten
				//Speicher, müssen wir ein redo machen.
				String Filename 	= String.valueOf(pid);
				int persistent_lsn 	= readLSNFromPage(Filename);
				
				System.out.println(persistent_lsn + " " + log_lsn);
				if(persistent_lsn < log_lsn){
					System.out.println("REDO at page:"+ pid);
					recoveryCount++;
				}
				
				}
				
				line = br.readLine();
			}
			
			System.out.println("Es mussten "+recoveryCount +" REDO's durchgeführt werden");
			}catch(IOException e){
				System.out.println(e.getMessage());
			}
	}
	
	public static void main(String[] argsv) {
	
		//Gehe Logdatei durch und finde committete Daten im Log, 
		//die noch nicht im persistenten Speicher sind, also persistent_lsn < log_lsn
		
		recover();
		
	}
}
