# setup intellij 
goto file -> project structure -> modules -> dependencies  
and add the ./lib folder as a dependency

# compile

```
javac -cp "./lib/*" -d build src/*
```

# run

rabbitmq server must be running 

## Receiver

```
java -cp "./build/:./lib/*" Recv
```

## Sender

```
java -cp "./build/:./lib/*" Send
```

# resources
https://www.rabbitmq.com/tutorials/tutorial-one-java.html  
https://www.rabbitmq.com/api-guide.html

