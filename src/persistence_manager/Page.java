package persistence_manager;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class Page {

	private int taid;
	private int pageid;
	private int lsn;
	private boolean commit;
	private String data;

	private static final String PREFIX = "Persistent";

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
	
	
	public static Page loadPersistantPageById(int pageid) {
		try (BufferedReader br = new BufferedReader(new FileReader(PREFIX + "/" + pageid))) {
			String line = br.readLine();
			String elements[] = line.split(",");
			int lsn = Integer.parseInt(elements[1]);
			String data = elements[2];
			return new Page(-1, pageid, lsn, data);
		} catch (FileNotFoundException  e) {
			System.out.println(e.getMessage());
			return null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(e.getMessage());
			return null;
		}

	}
}
