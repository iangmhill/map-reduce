javac -d . iMaster.java iMapper.java iReducer.java Master.java Mapper.java Reducer.java
java -classpath . -Djava.rmi.server.codebase=file:./ hadoop.Reducer 8002