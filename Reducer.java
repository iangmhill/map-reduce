package hadoop;

import java.rmi.NotBoundException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.Hashtable;

public class Reducer extends UnicastRemoteObject implements iReducer {
  private String word;
  private int value;
  private iMaster masterNode;

  public Reducer(String key, iMaster master) throws RemoteException {
    word = key;
    value = 0;
    masterNode = master;
  }

  public iReducer createReduceTask(String key, iMaster master) {
    Reducer reducer = null;
    try {
      reducer = new Reducer(key, master);
      System.out.println("Reduce task created");
    } catch (RemoteException e) {
      System.out.println(e.toString());
    }
    return reducer;
  }

  public void receiveValues(int value) {
    this.value += value;
  }

  public int terminate() {
    try {
      this.masterNode.receiveOutput(word, this.value);
      System.out.println("Reduce task terminated");
    } catch (RemoteException e) {
      System.out.println(e.toString());
    }
    return 0;
  }

  public static void main(String args[]) {
    try {
      // Initialize account information
      String selfPort = args[0];
      String selfIp = InetAddress.getLocalHost().getHostAddress();

      // Get the local registry
      Registry registry = LocateRegistry.getRegistry(selfIp, Integer.parseInt(selfPort));

      // Set up account
      Reducer reduceManager = new Reducer(null, null);

      // Bind the remote object's stub in the registry
      registry.bind("Reducer", reduceManager);
      System.out.println("Reducer ready");

    } catch (Exception e) {
      System.err.println("Connection exception: " + e.toString());
      e.printStackTrace();
    }
  }
}