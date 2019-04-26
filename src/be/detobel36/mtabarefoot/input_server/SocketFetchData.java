package be.detobel36.mtabarefoot.input_server;

import be.detobel36.mtabarefoot.TemporaryMemory;
import com.bmwcarit.barefoot.roadmap.RoadMap;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONException;
import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import org.slf4j.LoggerFactory;


/**
 *
 * @author remy
 */
public class SocketFetchData extends CustomTrackServer {
    
    private final static org.slf4j.Logger logger = LoggerFactory.getLogger(SocketFetchData.class);
    private final static String SOCKET_PATH = "/var/mtagrab/mtagrab.sock"; // 
    
    public SocketFetchData(final Properties properties, final RoadMap map,
            final TemporaryMemory.Publisher<CustomTrackServer.State> outputPublisher) {
        super(properties, map, outputPublisher);
    }
    
    
    @Override
    public void runServer() throws RuntimeException {
        final File socketFile = new File(SOCKET_PATH) ;
        try {
            final AFUNIXSocket sock = AFUNIXSocket.newInstance() ;
            sock.connect(new AFUNIXSocketAddress(socketFile)) ;
            final InputStream is = sock.getInputStream() ;
            System.gc();
            while(true) {
                final FeedMessage feed = FeedMessage.parseDelimitedFrom(is);
                logger.info("Fetch socket");
                Long startTime = System.currentTimeMillis();
                feed.getEntityList().forEach((entity) -> {
                    if(entity.hasVehicle()) {
                        final StringBuilder response = new StringBuilder();
                        try {
                            customResponseFactory.treatInformation(entity, response);
                        } catch (JSONException ex) {
                            Logger.getLogger(MTAFetchData.class.getName()).log(Level.SEVERE, null, ex);
                        }

                    } else {
                        logger.warn("Could not process following entity: " + entity.toString());
                    }

                });
                logger.info("End treat " + feed.getEntityCount() + " entity (" + ((System.currentTimeMillis()-startTime)/1000.0) + " sec)");
//                System.gc();
            }
        } catch (IOException ex) {
            Logger.getLogger(SocketFetchData.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    public int getPortNumber() {
        return -1;
    }
    
}
