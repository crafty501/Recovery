package persistence_manager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
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

	private int lsn;

	public static final String LOGFILENAME = "Log";
	public static final String COMMITFLAG = "c";

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
		lsn = 0;

		recover();

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

		logOut.println(COMMITFLAG + "," + taid);
		logOut.flush();
		activeTransactions.remove(activeTransactions.indexOf(taid));
	}

	@Override
	public synchronized void write(int taid, int pageid, String data) {

		int lsn = writeToLog(taid, pageid, data);

		// Simuliere einen System-Crash
		Random randomGenerator = new Random();
		int randomInt = randomGenerator.nextInt(20);
		if (randomInt == 6) {
			System.out.println("System-Crash");
			System.exit(0);
		}

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
		logOut.println(lsn + "," + taid + "," + pageid + "," + data);
		logOut.flush();
		return lsn;
	}

	/**
	 * Diese Methode geht die Log-File zeilenweise durch und vergleicht dabei
	 * die lsn des Log Eintrags mit der lsn der entsprechenden Page im
	 * persistenten Speicher Ist die LSN im Speicher kleiner als die in der
	 * Log-File müssen wir ein redo machen.
	 * 
	 */
	private void recover() {
		try (BufferedReader br = new BufferedReader(new FileReader(Manager.LOGFILENAME))) {

			int recoveryCount = 0;

			String line;
			while ((line = br.readLine()) != null) {

				String elements[] = line.split(",");
				if (elements[0].equals(Manager.COMMITFLAG)) {
					//System.out.println(elements[1]);
					int taid = Integer.parseInt(elements[1]);
					pageBuffer.forEach(page -> {
						if (page.getTaid() == taid) {
							page.setCommit();
						}
					});
				} else {
					// erstelle Page von der Log-File
					// [taid,pid,lsn,data]
					int lsn = Integer.parseInt(elements[0]);
					int taid = Integer.parseInt(elements[1]);
					int pageid = Integer.parseInt(elements[2]);
					String data = elements[3];

					Page logPage = new Page(taid, pageid, lsn, data);
					Page persPage = Page.loadPersistantPageById(pageid);
					if (persPage != null) {
						//System.out.println(logPage.getLsn() + " " + persPage.getLsn());

						// Wenn in der logfile eine neuere lsn gefunden wird, als im
						// persistenten
						// Speicher, müssen wir ein redo machen.
						if (logPage.getLsn() > persPage.getLsn()) {
							System.out.println("REDO at page:" + pageid);
							pageBuffer.add(logPage);
							recoveryCount++;
						}
					} else {
						System.out.println("REDO at page:" + pageid);
						pageBuffer.add(logPage);
						recoveryCount++;
					}
					
					//führe lsn alte weiter
					this.lsn = logPage.getLsn();
					
				}
			}

			System.out.println("Es mussten " + recoveryCount + " REDO's durchgeführt werden");
		} catch (IOException e) {
			System.err.println(e.getMessage());
		}
	}

}
