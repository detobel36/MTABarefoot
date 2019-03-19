package be.detobel36.mtabarefoot;

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
    
    private static CustomTrackServer trackerServer = null;
    
    
    private static void initServer(final String pathServerProperties, final String pathDatabaseProperties,
            final String typeServer) {
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

        
        map.construct();
        switch(typeServer.toLowerCase()) {
            case "server":
                trackerServer = new CustomTrackServer(serverProperties, map);
                break;
                
            case "mta":
                trackerServer = new MTAFetchData(serverProperties, map);
                break;
                
            default:
                logger.error("Unknow server type");
                System.exit(1);
                return;
        }
    }

    /**
     * Starts/runs server.
     */
    private static void runServer() {
        logger.info("starting server on port {} with map {}", trackerServer.getPortNumber(),
                databaseProperties.getProperty("database.name"));

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
        if (args.length != 3) {
            logger.error("missing arguments\nusage: /path/to/server/properties "
                    + "/path/to/mapserver/properties [server/mta]");
            System.exit(1);
        } else {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    stopServer();
                }
            });

            initServer(args[0], args[1], args[2]);
            runServer();
        }
    }
    
    
}
