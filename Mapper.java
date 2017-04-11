package hadoop;

import java.rmi.NotBoundException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.Hashtable;
import java.util.Set;

public class Mapper extends UnicastRemoteObject implements iMapper {

  public Mapper() throws RemoteException {
  }

  public iMapper createMapTask(String name) {
    Mapper mapper = null;
    try {
      mapper = new Mapper();
      System.out.println("Map task created");
    } catch (RemoteException e) {
      System.out.println(e.toString());
    }
    return mapper;
  }

  public void processInput(String input, iMaster theMaster) {
    Hashtable<String, Integer> freq = new Hashtable<String, Integer>();
    String[] split = input.toLowerCase().replaceAll("[^a-zA-Z\\s]", "").split("\\s");
    for (int i = 0; i < split.length; i++) {
      if(!freq.containsKey(split[i])){ //if new word
        freq.put(split[i], 1);
      }
      else { // if word already exists in hashmap
        int prevFrequency = freq.get(split[i]);
        freq.put(split[i], ++prevFrequency);
      }
    }
    Set<String> keySet = freq.keySet();
    String[] keys = keySet.toArray(new String[keySet.size()]);
    try {
      iReducer[] reducers = theMaster.getReducers(keys);
      for (int i = 0; i < keys.length; i++) {
        reducers[i].receiveValues(freq.get(keys[i]));
      }
      theMaster.markMapperDone();
      System.out.println("Map task done");
    } catch (Exception e) {
      System.err.println("Failed to get or send to reducers: " + e.toString());
      e.printStackTrace();
    }
  }

  public static void main(String args[]) {
    try {
      // Initialize account information
      String selfPort = args[0];
      String selfIp = InetAddress.getLocalHost().getHostAddress();

      // Get the local registry
      Registry registry = LocateRegistry.getRegistry(selfIp, Integer.parseInt(selfPort));

      // Set up account
      Mapper mapManager = new Mapper();

      // Bind the remote object's stub in the registry
      registry.bind("Mapper", mapManager);
      System.out.println("Mapper ready");

    } catch (Exception e) {
      System.err.println("Connection exception: " + e.toString());
      e.printStackTrace();
    }
  }
}