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
 * Klasse wird nicht mehr benutzt
 * 
 *
 */


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
		
		Page p = new Page(taid,pageId,lsn,data);
		pageList.add(p);
	
	}
	
	public void write(int taid,int pageId,String data){	
		boolean written = false;
		int lsn_file = readLSNFromPage(String.valueOf(pageId));
		for (int i = 0 ; i < pageList.size() ; i++){
			int pid = pageList.get(i).getPageid();
			int lsn = pageList.get(i).getLsn();
			//Update data 
			//System.out.println(pid + " "+ pageId);
			if((pid == pageId)){
				System.out.println("Update");
				System.exit(0);
				lsn = lsn_file +1;	
				Page p = new Page(taid,pid, lsn, data) ;
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
					Page p = pageList.get(x);
					if(p.getTaid() == taid){
						pageList.get(x).setCommit();
					}
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
			if(pageList.get(i).isCommit()){
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
			if(p.isCommit()){
				String Filename = String.valueOf(p.getPageid());
				String Line 	= p.toString();
				//Schreibe Datei auf die Festplatte
				p.persist();
				//TODO Nach dem write das Element an dieser Stelle aus dem Buffer löschen 
				pageList.remove(i);
				i--;
			}
		}
	}

	
	public void writeToLog(int taid){
		for(int i = 0 ; i  < pageList.size(); i++){
		Page p = pageList.get(i);
		if(p.isCommit()){
			int pid = p.getPageid();
			
			int lsn = p.getLsn();
			String data = p.getData();
			
			String Line =  taid + "," + pid + "," +lsn + ","+ data;
			addLog(Line);
			System.out.print("..done");
			
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
