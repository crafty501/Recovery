package main;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import persistence_manager.IManager;
import persistence_manager.Manager;

public class Main {

	static IManager m;
	static List<Client> clientList;
	static int start;
	
	public static void main(String[] argsv) {
		
		m = Manager.getInstance();
		clientList = new ArrayList<Client>();
		
		int i = 0; 
		while (i < 20){
			start = 10 + (i* 10) ;
			//stop  = start + 9;
			Client c = new Client(m,start);
			c.start();
			i++;
		}
	}
}
