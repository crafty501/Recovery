package persistence_manager;

public class Page {

	int 		taid;
	int 		pid;
	int 		lsn;
	boolean 	commit;
	String 		data;
	
	public Page(int _taid,int _pid , int _lsn , boolean _commit, String _data){
		taid = _taid;
		pid = _pid;
		lsn = _lsn;
		commit = _commit;
		data = _data;
	}
	
	public int getPid(){
		return pid;
	}
	
	
	public int getLsn(){
		return lsn;
	}
	
	public String toString(){
		return pid + "," + lsn + "," + data;
	}
	
	public void setCommit(){
		commit= true;
	}
	
}
