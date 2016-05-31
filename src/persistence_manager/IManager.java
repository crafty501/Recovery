package persistence_manager;

public interface IManager {

	
	public int beginTransaction();
	public void commit(int taid);
	
	/**
	 * Initially, user data that have been modified by a write 
	 * operation are stored to the internal buffer of the 
	 * persistence manager. Already existing versions can be 
	 * overwritten directly in the buffer without having been 
	 * written to the persistent storage.
	 * If the buffer contains more than five datasets after a
	 * write operation, the datasets corresponding to a committed 
	 * transaction are written directly to the persistent storage 
	 * (non-atomic)
	 * @param taid
	 * @param pageid
	 * @param data
	 */
	public void write(int taid, int pageid, String data);

}
