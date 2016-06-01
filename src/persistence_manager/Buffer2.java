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


public class Buffer2 {

	
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
		prefix 			= "Memory";
		log = new ArrayList<String>();
		pageList = new ArrayList<Page>();
		
	}
	
	private void append(int taid,int pageId,int lsn, String data){
		
		Page p = new Page(taid,pageId,lsn,false,data);
		pageList.add(p);
	
	}
	
	public void write(int taid,int pageId,String data){	
		boolean written = false;
		int lsn_file = readLSNFromPage(String.valueOf(pageId));
		for (int i = 0 ; i < pageList.size() ; i++){
			int pid = pageList.get(i).getPid();
			int lsn = pageList.get(i).getLsn();
			//Update data 
			if((pid == pageId)){
				lsn = lsn_file +1;	
				Page p = new Page(taid,pid, lsn, false, data) ;
				pageList.set(i, p);
				written = true;
			}
		}
		//Append data
		//Wenn ein Eintrag komplett neu, d.h es gibt keine page im Hauptspeicher
		if(!written){
			//LSN auslesen 
			append(taid,pageId, lsn_file + 1, data);
		}
		
		
	}
	
	/**
	 * Diese Methode setzt nur das Commit Flag
	 */
	public void commit(int taid){
		
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
			return 0;
			
		}
		
		
	}
	
	public int countCommitFlags(){
		
		int ccount = 0 ;
		for(int i = 0; i < pageList.size() ; i++){
			if(pageList.get(i).getCommitState()){
				ccount++;
			}
		}
		return ccount;
	}
	
	
	public int size(){
		return pageList.size();
	}
	
	
	public void writetoPersistent(){
		int index = 0;
		for(int i = 0; i < pageList.size(); i++){
			Page p = pageList.get(i);
			if(p.getCommitState()){
				String Filename = String.valueOf(p.getPid());
				String Line 	= p.toString();
				//Schreibe Datei auf die Festplatte
				p.writePage(Filename, Line);
				//TODO Nach dem write das element an dieser Stelle aus dem Buffer löschen 
				pageList.remove(i);
				i--;
			}
			
		}
	}

	
	public void writeToLog(int taid){
		for(int i = 0 ; i  < pageList.size(); i++){
		Page p = pageList.get(i);
		if(p.getCommitState()){
			int pid = p.getPid();
			int lsn = p.getLsn();
			String data = p.getData();
			String Line = "[" + taid + "," + pid + "," +lsn + ","+ data +"]";
			addLog(Line);
			
		}
			
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
	
	
	
	public void print_log(){
	
		for(int i = 0 ; i < log.size() ; i++){
			String Zeile = log.get(i);
			System.out.println(Zeile);
		}
	}
}
