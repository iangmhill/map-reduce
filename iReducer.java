package hadoop;

import java.rmi.*;
import java.rmi.server.*;
import java.rmi.RemoteException;

import java.util.*;

public interface iReducer extends Remote {
  public iReducer createReduceTask(String key, iMaster master) throws RemoteException, AlreadyBoundException;
  public void receiveValues(int value) throws RemoteException;
  public int terminate() throws RemoteException;
}