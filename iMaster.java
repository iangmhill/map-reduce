package hadoop;

import java.rmi.*;
import java.rmi.server.*;
import java.rmi.RemoteException;

import java.util.*;

public interface iMaster extends Remote {
  public iReducer[] getReducers(String[] keys) throws RemoteException, AlreadyBoundException;
  public void markMapperDone() throws RemoteException;
  public void receiveOutput(String key, int value) throws RemoteException;
}