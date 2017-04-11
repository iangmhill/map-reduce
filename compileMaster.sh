javac -d . iMaster.java iMapper.java iReducer.java Master.java Mapper.java Reducer.java
java -classpath . -Djava.rmi.server.codebase=file:./ hadoop.Master 8000 127.0.1.1:8001 127.0.1.1:8002