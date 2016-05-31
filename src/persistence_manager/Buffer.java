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
public class Buffer {

	//Page ID
	//Log sequence number
	//Data
	
	boolean[] 		change_array;		// Deises Array speicher ob eine veränderung stattgefunden hat
	int 			last_element;
	int 			max_index;
	int 			lineNumber;
	List<Integer> 	pageIdList;
	List<Integer> 	lsnlist;
	List<String>	dataList;

	List<String> 	log;
	int 			log_index;
	int 			taid;
	private String getElement(int index){
		
		int pageID 	= pageIdList.get(index);
		int lsn		= lsnlist.get(index);
		String data = dataList.get(index);
		
		return pageID + "," + lsn + "," + data;
	}
	/**
	 * Erzeugt einen leeren Buffer.
	 */
	public Buffer(int _taid){
		super();
		//Index-Array Initialisieren
		max_index 		= 1000;
	    last_element 	= 0;	
		taid 			= _taid;
		
		pageIdList 	= new ArrayList<Integer>();
		lsnlist 	= new ArrayList<Integer>();
		dataList	= new ArrayList<String>();
		
		//Dies sollte atomar sein
		lineNumber = lineNumbers();
		load();
		
		change_array = new boolean[lineNumber];
		for (int i = 0; i < lineNumber ; i++){
			change_array[i]= false;
		}
		
		log = new ArrayList<String>();
		
	}
	
	private void append(int pageId,int lsn, String data){
		pageIdList.add(pageId);
		lsnlist.add(lsn);
		dataList.add(data);
		last_element++;
	}
	/**
	 * Diese Funktion muss die pages aus dem persistenten Speicher laden.
	 * Der persistente Speicher soll als Dateien auf der Festplatte 
	 * representiert werden 
	 */
	private void load(){
		
		try {
			String Filename = "persistent_memory";
			BufferedReader br = new BufferedReader(new FileReader(Filename));
		    String line = br.readLine();
		    while (line != null) {
		    	String[] fields = line.split(",");
		    	int pageid		= Integer.parseInt(fields[0]);
		    	int lsn 		= Integer.parseInt(fields[1]);	 
		        append(pageid, lsn, fields[2]);
		        line = br.readLine();
		    }
		    br.close();
		}catch (IOException e){
			System.out.println(e.getMessage());
		}
	}
	
	
	
	
	
	public void write(int pageId,String Data){	
		boolean written = false;
		for (int i = 0 ; i < last_element ; i++){
			int _pid = this.pageIdList.get(i);
			int _lsn = this.lsnlist.get(i);
			//Update data 
			if((_pid == pageId)){
				change_array[i] = true;
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
		
		
		
		//Find the lines to change
	
		for (int i = 0 ; i < change_array.length; i++){
			if(change_array[i]){
				int pid 	= pageIdList.get(i);
				int lsn 	= lsnlist.get(i);
				String data = dataList.get(i);
				String x = pid + "," + lsn + "," + data;
				//Logs schreiben
				
				String slog = "["+lsn+","+taid+","+pid+","+data+"]";
				log.add(slog);
				addLog(slog);
				
				replaceLine(i,x);
			}
		}
		
		//Find Lines to append
		int toappend = dataList.size() - lineNumber;
		//System.out.println(toappend);
		if(toappend > 0 ){
			for(int x = dataList.size() - toappend; x < dataList.size(); x++ ){
				int pid 	= pageIdList.get(x);
				int lsn 	= lsnlist.get(x);
				String data = dataList.get(x);
				String append = pid + "," + lsn + "," + data;
				
				//Logs schreiben
				String slog = "["+lsn+","+taid+","+pid+","+data+"]";
				log.add(slog);
				
				addLog(slog);
				addLine(append);
				//System.out.println("append:"+append);
			}
		}
	}
	
	
	public int lineNumbers(){
		int c = -1;
		try {
			String Filename = "persistent_memory";
			BufferedReader br = new BufferedReader(new FileReader(Filename));
			
			String line = "";
			while(line != null){
				line = br.readLine();
				c++;
			}
			
			
		}catch(IOException w ){
			System.out.println(w.getMessage());
		}
		
		return c;
		
	}
	
	
	private void replaceLine(int n,String Line){
		try {
			String Filename = "persistent_memory";
			BufferedReader br = new BufferedReader(new FileReader(Filename));
		    
		    int i = 0; 
		    int c = 0;
		    String[] lines = new String[6000];
		    String line = "";
		    while (line != null) {
		    	if(i != n){
		        line = br.readLine();
		        if(line != null){
		        	lines[c] = line;
		        	c++;
		        }
		    	}else{
		    		line = br.readLine();
		    		lines[c] = Line;
		    		c++;
		    	}
		    	i++;
		    }
		    br.close();
		    
		    FileWriter fw = new FileWriter(Filename);
		    fw.flush();
		    i = 0;
		    String x = lines[i];
			while(x != null){
				
				if(i < lineNumber -1 ){
					fw.write(x+"\n");
				}else{
					fw.write(x);	
				}
				i++;
				x = lines[i];
			}
			fw.close();
		}catch (IOException e){
			System.out.println(e.getMessage());
		}}
	
	private void deleteLine(int n){
		try {
			String Filename = "persistent_memory";
			BufferedReader br = new BufferedReader(new FileReader(Filename));
		    
		    int i = 0; 
		    int c = 0;
		    String[] lines = new String[6000];
		    String line = "";
		    while (line != null) {
		    	if(i != n){
		        line = br.readLine();
		        if(line != null){
		        	lines[c] = line;
		        	c++;
		        }
		    	}else{
		    		line = br.readLine();
		    	}
		    	i++;
		    }
		    br.close();
		    
		    FileWriter fw = new FileWriter(Filename);
		    fw.flush();
		    i = 0;
		    String x = lines[i];
			while(x != null){
				if(i < lineNumber -1 ){
					fw.write(x+"\n");
				}else{
					fw.write(x);	
				}
				i++;
				x = lines[i];
			}
			fw.close();
		}catch (IOException e){
			System.out.println(e.getMessage());
		}}
	
	private void addLine(String Line){
	try{
		
		String Filename = "persistent_memory";
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
		
		for (int i = 0 ; i < last_element; i++){
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
