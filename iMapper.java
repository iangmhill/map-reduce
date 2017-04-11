package hadoop;

import java.rmi.*;
import java.rmi.server.*;
import java.rmi.RemoteException;

import java.util.*;

public interface iMapper extends Remote {
  public iMapper createMapTask(String name) throws RemoteException, AlreadyBoundException;
  public void processInput(String input, iMaster theMaster) throws RemoteException, AlreadyBoundException;
}