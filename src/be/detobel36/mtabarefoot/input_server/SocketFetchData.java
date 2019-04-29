package be.detobel36.mtabarefoot.input_server;

import be.detobel36.mtabarefoot.TemporaryMemory;
import com.bmwcarit.barefoot.roadmap.RoadMap;
import com.google.transit.realtime.GtfsRealtime;
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
    
    private boolean finishTreatInformation = true;
    private FeedMessage lastFeed = null;
    
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
                try {
                    final FeedMessage feed = FeedMessage.parseDelimitedFrom(is);
                    lastFeed = feed;
                    if(finishTreatInformation) {
                        finishTreatInformation = false;
                        treatReceiveInformation();
                    } else {
                        logger.info("Fetch but not treat");
                    }
                    
                } catch (IOException ex) {
                    Logger.getLogger(SocketFetchData.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(SocketFetchData.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void treatReceiveInformation() {
        new Thread() {
            @Override
            public void run() {
                logger.info("New treat socket information");
                
                Long totalTimeTreatInfo = 0l;
                int nbrElement = 0;
                
                while(lastFeed != null) {
                    final FeedMessage feed = lastFeed;
                    lastFeed = null;
                    logger.info("Treat socket information");
                    Long startTime = System.currentTimeMillis();
                    for(final GtfsRealtime.FeedEntity entity : feed.getEntityList()) {
                        if(entity.hasVehicle()) {
                            final StringBuilder response = new StringBuilder();
                            try {
                                final long startTreat = System.currentTimeMillis();
                                customResponseFactory.treatInformation(entity, response);
                                final long timeTreatInfo = System.currentTimeMillis()-startTreat;
                                totalTimeTreatInfo += timeTreatInfo;
                                ++nbrElement;
                                
                            } catch (JSONException ex) {
                                Logger.getLogger(MTAFetchData.class.getName()).log(Level.SEVERE, null, ex);
                            }

                        } else {
                            logger.warn("Could not process following entity: " + entity.toString());
                        }
                    }
                    logger.info("End treat " + feed.getEntityCount() + " entity " + 
                            "(" + ((System.currentTimeMillis()-startTime)/1000.0) + 
                            " sec)");
                    logger.info("Temps moyen de traitement d'un élément: " + (totalTimeTreatInfo/nbrElement));
                    logger.info(" ");
                    logger.info(" ");
                    logger.info("----------------------------------------------");
                }
                finishTreatInformation = true;
            }
        }.start();
    }
    
    @Override
    public int getPortNumber() {
        return -1;
    }
    
}
