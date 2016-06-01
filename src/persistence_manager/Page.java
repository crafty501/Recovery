package persistence_manager;

import java.io.FileWriter;
import java.io.IOException;

public class Page {

	int 		taid;
	int 		pid;
	int 		lsn;
	boolean 	commit;
	String 		data;
	String 		prefix;
	
	public Page(int _taid,int _pid , int _lsn , boolean _commit, String _data){
		taid = _taid;
		pid = _pid;
		lsn = _lsn;
		commit = _commit;
		data = _data;
		prefix	= "Memory";
	}
	
	public int getPid(){
		return pid;
	}
	
	
	public int getLsn(){
		return lsn;
	}
	
	public String getData(){
		return data;
	}
	
	public String toString(){
		return pid + "," + lsn + "," + data;
	}
	
	public void setCommit(){
		commit= true;
	}
	
	public boolean getCommitState(){
		return commit;
	}
	
	
	public void writePage(String Filename,String Line){
		try {
			String f = prefix+"/"+Filename;
			FileWriter fw = new FileWriter(f);
		    fw.flush();
		    fw.write(Line);
			fw.close();
		}catch (IOException e){
			System.out.println(e.getMessage());
		}}
}
