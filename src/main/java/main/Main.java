package main;

import local.Local;
import manager.Manager;
import worker.Worker;

public class Main {

	public static void main(String[] args) throws Exception {
		if (args.length == 0 || args.length > 2)
			Local.main(args);
		else if (args[0].equals("manager"))
			Manager.main(args);
		else
			Worker.main(args);
	}

}
