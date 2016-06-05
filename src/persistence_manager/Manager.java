package persistence_manager;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class Manager implements IManager {

	private List<Integer> activeTransactions;

	private List<Page> pageBuffer;

	private static int lsn = 0;


	private static final String LOGFILENAME = "Log";
	PrintWriter logOut;

	static final private Manager manager;

	static {
		try {
			manager = new Manager();
		} catch (Throwable e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	static public Manager getInstance() {
		return manager;
	}

	private Manager() {

		activeTransactions = new ArrayList<Integer>();
		pageBuffer = new ArrayList<Page>();

		try {
			FileWriter fw = new FileWriter(LOGFILENAME);
			BufferedWriter bw = new BufferedWriter(fw);
			logOut = new PrintWriter(bw);
		} catch (IOException e) {
			System.err.println("logfile init error");
			e.printStackTrace();
		}
		
	}

	@Override
	public synchronized int beginTransaction() {
		// Vergebe eindeutige Transaction ID

		Random randomGenerator = new Random();
		int randomInt = randomGenerator.nextInt();

		while (activeTransactions.contains(randomInt)) {
			randomInt = randomGenerator.nextInt(500);
		}

		activeTransactions.add(randomInt);

		return randomInt;
	}

	@Override
	public synchronized void commit(int taid) {
		// The log is produced during the commit
		pageBuffer.forEach(page -> {
			if (page.getTaid() == taid) {
				page.setCommit();
			}
		});
		
		logOut.println(taid + ": commited");
		logOut.flush();
		activeTransactions.remove(activeTransactions.indexOf(taid));
	}

	@Override
	public synchronized void write(int taid, int pageid, String data) {

		int lsn = writeToLog(taid, pageid, data);

		
		Page newpage = new Page(taid, pageid, lsn, data);
		
		boolean isupdated = false;
		for (int i = 0; i < pageBuffer.size(); i++) {
			Page page = pageBuffer.get(i);
			if (page.getPageid() == newpage.getPageid()) {
				page.update(newpage);
				isupdated = true;
			}
		}
		if (!isupdated) {
			pageBuffer.add(newpage);
		}
		
				
		if (pageBuffer.size() > 5) {
			for (Iterator<Page> i = pageBuffer.iterator(); i.hasNext();) {
				Page page = i.next();
			    if (page.isCommit()) {
			    	page.persist();
			        i.remove();
			    }
			}
		}
	}

	private int writeToLog(int taid, int pageid, String data) {
		lsn++;
		logOut.println(lsn + "," + taid + "," + pageid + ","+data);
		logOut.flush();
		return lsn;
	}


}
