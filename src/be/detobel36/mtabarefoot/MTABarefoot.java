package be.detobel36.mtabarefoot;

import be.detobel36.mtabarefoot.input_server.CustomTrackServer;
import be.detobel36.mtabarefoot.input_server.MTAFetchData;
import be.detobel36.mtabarefoot.input_server.SocketFetchData;
import be.detobel36.mtabarefoot.publisher.StatePublisherExternalTcp;
import be.detobel36.mtabarefoot.publisher.StatePublisherPostgreSQL;
import com.bmwcarit.barefoot.roadmap.Loader;
import com.bmwcarit.barefoot.roadmap.RoadMap;
import com.bmwcarit.barefoot.util.SourceException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author remy
 */
public class MTABarefoot {
    
    private static final Logger logger = LoggerFactory.getLogger(MTABarefoot.class);
    
    private static final Properties databaseProperties = new Properties();
    private static final Properties serverProperties = new Properties();
    private static String requestPath = "./config/request.sql";
    
    private static CustomTrackServer trackerServer = null;
    private static TemporaryMemory.Publisher<CustomTrackServer.State> outputPublisher;
    
    private static boolean view_query = false;
    
    private static void initServer(final String pathServerProperties, final String pathDatabaseProperties,
            final String typeServerInput, final String typeServerOutput) {
        logger.info("initialize server");

        try {
            logger.info("read database properties from file {}", pathDatabaseProperties);
            databaseProperties.load(new FileInputStream(pathDatabaseProperties));
        } catch (FileNotFoundException e) {
            logger.error("file {} not found", pathDatabaseProperties);
            System.exit(1);
            return;
        } catch (IOException e) {
            logger.error("reading database properties from file {} failed: {}",
                    pathDatabaseProperties, e.getMessage());
            System.exit(1);
            return;
        }
        
        try {
            logger.info("read tracker properties from file {}", pathServerProperties);
            serverProperties.load(new FileInputStream(pathServerProperties));
        } catch (FileNotFoundException e) {
            logger.error("file {} not found", pathServerProperties);
            System.exit(1);
            return;
        } catch (IOException e) {
            logger.error("reading tracker properties from file {} failed: {}",
                    pathDatabaseProperties, e.getMessage());
            System.exit(1);
            return;
        }

        final RoadMap map;
        try {
            map = Loader.roadmap(databaseProperties, true);
        } catch (SourceException e) {
            logger.error(e.getMessage());
            System.exit(1);
            return;
        }
        
        
        switch(typeServerOutput.toLowerCase()) {
            case "tcp":
                outputPublisher = new StatePublisherExternalTcp();
                break;
                
            case "sql":
                outputPublisher = new StatePublisherPostgreSQL(databaseProperties, requestPath);
                break;

                
            default:
                logger.error("Unknow OUTPUT server type");
                System.exit(1);
                return;
        }
        
        map.construct();
        
        switch(typeServerInput.toLowerCase()) {
            case "server":
                trackerServer = new CustomTrackServer(serverProperties, map, outputPublisher);
                break;
                
            case "mta":
                trackerServer = new MTAFetchData(serverProperties, map, outputPublisher);
                break;
                
            case "sock":
            case "socket":
                trackerServer = new SocketFetchData(serverProperties, map, outputPublisher);
                break;
                
            default:
                logger.error("Unknow INPUT server type");
                System.exit(1);
        }
    }

    /**
     * Starts/runs server.
     */
    private static void runServer() {
        if(trackerServer.getPortNumber() > 0) {
            logger.info("starting server on port {} with map {}", trackerServer.getPortNumber(),
                databaseProperties.getProperty("database.name"));
        } else {
            logger.info("starting server with map {}", databaseProperties.getProperty("database.name"));
        }

        trackerServer.runServer();
        logger.info("server stopped");
    }

    /**
     * Stops server.
     */
    private static void stopServer() {
        logger.info("stopping server");
        if (trackerServer != null) {
            trackerServer.stopServer();
        } else {
            logger.error("stopping server failed, not yet started");
        }
    }
    
    
    public static void main(String[] args) {
        if (args.length != 4) {
            logger.error("missing arguments\nusage: </path/to/server/properties> "
                    + "</path/to/mapserver/properties> [INPUT: server/mta/socket] [OUTPUT: tcp/sql]");
            System.exit(1);
        } else {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    stopServer();
                }
            });
            
            new ScanIn();
            
            initServer(args[0], args[1], args[2], args[3]);
            runServer();
        }
    }
    
    protected static void reloadOutput() {
        outputPublisher.reload();
    }
    
    public static String getRequestPath() {
        return requestPath;
    }
    
    public static void switchViewQuery() {
        view_query = !view_query;
    }
    
    public static boolean viewQuery() {
        return view_query;
    }
    
}
