package persistence_manager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.io.File;
import java.io.FileNotFoundException;


/**
 * Dabei werrden alle Daten in mehreren Arrays 
 * gespeichert, welche über den index die Daten PageID,LSN,DATA
 * miteinander verknüpfen. 
 * 
 * Initially, user data that have been modified by a write operation 
 * are stored to the internal buffer of the persistence manager
 * Already existing versions can be overwritten directly in the buffer without
 * having been written to the persistent storage.
 * A Page is represented by a file with the name pageid
 * @author callya
 *
 */
public class Buffer2 {

	//Page ID
	//Log sequence number
	//Data
	
	//int 			last_element;
	int 			max_index;
	int 			lineNumber;
	
	List<Page>      pageList;
	
    List<String> 	log;
	int 			log_index;
	String 			prefix;
	
	
	/**
	 * Erzeugt einen leeren Buffer.
	 */
	public Buffer2(){
		super();
		//Index-Array Initialisieren
		max_index 		= 1000;
	    //last_element 	= 0;	
		//taid 			= _taid;
		prefix 			= "Memory";
		log = new ArrayList<String>();
		
	}
	
	private void append(int taid,int pageId,int lsn, String data){
		
		Page p = new Page(taid,pageId,lsn,false,data);
		pageList.add(p);
	
	}
	
	public void write(int taid,int pageId,String data){	
		boolean written = false;
		
		
		
		
		for (int i = 0 ; i < pageList.size() ; i++){
			int pid = pageList.get(i).getPid();
			int lsn = pageList.get(i).getLsn();
			//Update data 
			if((pid == pageId)){
				lsn++;	
				Page p = new Page(taid,pid, lsn, false, data) ;
				pageList.set(i, p);
				written = true;
			}
		}
		//Append data
		//Wenn ein Eintrag komplett neu ist
		if(!written){
			//LSN auslesen 
			int lsn_file = readLSNFromPage(String.valueOf(pageId));
			append(taid,pageId, lsn_file, data);
		}
		
		
	}
	
	/**
	 * Diese funktion schreibt den buffer in den persistenten speicher
	 */
	public void commit(int taid){
		
		//Setze das Commit Flag
			for(int x = 0; x < pageList.size(); x++ ){
					pageList.get(x).setCommit();
				}
	}
			
	private int readLSNFromPage(String Filename){
		
		
		try{
		BufferedReader br = new BufferedReader( new FileReader(prefix+"/"+Filename));
		String line = br.readLine();
		
		String elements[] = line.split(",");
		
		return Integer.parseInt(elements[1]);
		}catch(IOException e){
			return -1;
			
		}
		
		
	}
	
	private void writePage(String Filename,String Line){
		try {
			String f = prefix+"/"+Filename;
			FileWriter fw = new FileWriter(f);
			
		    fw.flush();
		    fw.write(Line);
			fw.close();
		}catch (IOException e){
			System.out.println(e.getMessage());
		}}
	
	
	
	
	
	private void addLog(String Line){
		try{
			
			String Filename = "Log";
			BufferedReader br = new BufferedReader(new FileReader(Filename));
			
			String[] lines = new String[6000];
			String line = br.readLine();
			int c = 0;
			while(line != null){
				lines[c] = line;
				line = br.readLine();
				c++;
			}
			//System.out.println(c);
			FileWriter fw = new FileWriter(Filename);
		    for (int i = 0 ; i < c ; i++){
		    	fw.write(lines[i]+"\n");
		    }
		    fw.write(Line);
			fw.close();
		}catch (IOException e){
			System.out.println(e.getMessage());
		}
		
		}
	
	
	
	public void print_log(){
	
		for(int i = 0 ; i < log.size() ; i++){
			String Zeile = log.get(i);
			System.out.println(Zeile);
		}
	}
}
