package persistence_manager;

import java.io.FileWriter;
import java.io.IOException;

public class Page {

	private int taid;
	private int pageid;
	private int lsn;
	private boolean commit;
	private String data;

	private static final String PREFIX = "Memory";

	public Page(int taid, int pageid, int lsn, String data) {
		this.taid = taid;
		this.pageid = pageid;
		this.lsn = lsn;
		this.data = data;
	}

	public int getPageid() {
		return pageid;
	}

	public int getTaid() {
		return taid;
	}

	public int getLsn() {
		return lsn;
	}

	public String getData() {
		return data;
	}

	public String toString() {
		return pageid + "," + lsn + "," + data;
	}

	public void setCommit() {
		commit = true;
	}

	public boolean isCommit() {
		return commit;
	}

	public void persist() {
		
		String Filename = String.valueOf(pageid);
		
		try {
			String f = PREFIX + "/" + Filename;
			FileWriter fw = new FileWriter(f);
			fw.flush();
			fw.write(this.toString());
			fw.close();
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}

	public void update(Page newpage) {
		lsn = newpage.getLsn();
		data = newpage.getData();
		taid = newpage.getTaid();
	}
}
