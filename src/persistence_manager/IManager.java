package persistence_manager;

public interface IManager {

	
	public int beginTransaction();
	public void commit(int taid);
	public void write(int taid, int pageid, String data);

}
