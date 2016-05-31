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
	
	List<Boolean>	change_list;		// Deises Array speicher ob eine veränderung stattgefunden hat
	//int 			last_element;
	int 			max_index;
	int 			lineNumber;
	List<Integer> 	pageIdList;
	List<Integer> 	lsnlist;
	List<String>	dataList;

	List<String> 	log;
	int 			log_index;
	int 			taid;
	String 			prefix;
	private String getElement(int index){
		
		int pageID 	= pageIdList.get(index);
		int lsn		= lsnlist.get(index);
		String data = dataList.get(index);
		
		return pageID + "," + lsn + "," + data;
	}
	/**
	 * Erzeugt einen leeren Buffer.
	 */
	public Buffer2(int _taid){
		super();
		//Index-Array Initialisieren
		max_index 		= 1000;
	    //last_element 	= 0;	
		taid 			= _taid;
		prefix 			= "Memory";
		pageIdList 	= new ArrayList<Integer>();
		lsnlist 	= new ArrayList<Integer>();
		dataList	= new ArrayList<String>();
		
		//Dies sollte atomar sein
		load();
		lineNumber = dataList.size();
		
		//System.out.println("lineNumber: "+lineNumber);
		//System.exit(0);
		change_list = new ArrayList<Boolean>();
		for (int i = 0; i < lineNumber ; i++){
			change_list.add(false);
		}
		
		log = new ArrayList<String>();
		
	}
	
	private void append(int pageId,int lsn, String data){
		pageIdList.add(pageId);
		lsnlist.add(lsn);
		dataList.add(data);
		//last_element++;
	}
	/**
	 * Diese Funktion muss die pages aus dem persistenten Speicher laden.
	 * Der persistente Speicher soll als Dateien auf der Festplatte 
	 * representiert werden 
	 */
	private void load(){
		
		String[] entries = new File( prefix + "/." ).list();
		for(int i = 0 ; i < entries.length ; i++){
			String filename = prefix + "/" + entries[i];
			//System.out.println(filename);
			try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			
		    String line 	= br.readLine();
			String[] fields = line.split(",");
	    	int pageid		= Integer.parseInt(fields[0]);
	    	int lsn 		= Integer.parseInt(fields[1]);	 
	        append(pageid, lsn, fields[2]);
	        //System.out.println(fields[2]);
	        br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		
	}
	
	public void write(int pageId,String Data){	
		boolean written = false;
		for (int i = 0 ; i < dataList.size() ; i++){
			int _pid = this.pageIdList.get(i);
			int _lsn = this.lsnlist.get(i);
			//Update data 
			if((_pid == pageId)){
				change_list.set(i, true);
				dataList.set(i, Data);
				_lsn++;
				lsnlist.set(i, _lsn);
				written = true;
			}
		}
		//Append data
		//Wenn ein Eintrag komplett neu ist
		if(!written){
			append(pageId, 0, Data);
		}
		
		
	}
	
	/**
	 * Diese funktion schreibt den buffer in den persistenten speicher
	 */
	public void commit(){
		//Override old/existing Page	
			for(int x = 0; x < dataList.size(); x++ ){
				if(change_list.get(x)){
					int pid 	= pageIdList.get(x);
					int lsn 	= lsnlist.get(x);
					String data = dataList.get(x);
					String append = pid + "," + lsn + "," + data;
				
					//Logs schreiben
					String slog = "["+lsn+","+taid+","+pid+","+data+"]";
					log.add(slog);
				
					addLog(slog);
					writePage(String.valueOf(pid),append);
					//System.out.println("append:"+append);
				}
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
	
	
	public void print(){
	
		for (int i = 0 ; i < dataList.size(); i++){
			String Zeile = getElement(i);
			System.out.println(Zeile);
		}
	}
	
	public void print_log(){
	
		for(int i = 0 ; i < log.size() ; i++){
			String Zeile = log.get(i);
			System.out.println(Zeile);
		}
	}
}
