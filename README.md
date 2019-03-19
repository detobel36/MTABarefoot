# MTABarefoot

This project uses the [Barefoot library](https://github.com/bmwcarit/barefoot).  It allows you to retrieve more information from the output but also to automatically retrieve the position of New York buses (provided by [MTA](http://web.mta.info/developers/)).


## File structure
- **src**
    Folder which contain all code
- **uml**
    Contain some UML schema (maybe not up-to-date)
- **barefoot.sh**
    Script to launch/install barefoot element


## Start MTABarefoot

Before to launch the program, you need to build it. You can use Maven:
```
mvn clean install
```
After that you can execute the following command:
```
java -jar mtabarefoot-jar-with-dependencies.jar config/tracker.properties config/customConfig.properties mta
```
In parameter you can choose the track and the database properties. The last parameter allow you to choose which type of server:
- server
- mta

The first one allow different system to send information (in socket). The second will fetch automatically information on MTA servers.

