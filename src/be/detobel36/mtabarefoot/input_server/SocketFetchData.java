package be.detobel36.mtabarefoot.input_server;

import be.detobel36.mtabarefoot.TemporaryMemory;
import be.detobel36.mtabarefoot.input_server.CustomTrackServer;
import be.detobel36.mtabarefoot.input_server.MTAFetchData;
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
            while(true) {
                final FeedMessage feed = FeedMessage.parseDelimitedFrom(is);
              
                feed.getEntityList().forEach((entity) -> {
                    if(entity.hasVehicle()) {
                        
//                        
//                        final GtfsRealtime.VehiclePosition vehicle = entity.getVehicle();
//                        final GtfsRealtime.Position pos = vehicle.getPosition();
//                        final long time = vehicle.getTimestamp() * 1000; // sec to milisecond
//                        final String id = vehicle.getVehicle().getId();
//                        final int directionId = vehicle.getTrip().getDirectionId();
//                        
//
//                        final String point = "POINT(" + pos.getLongitude() + " " + pos.getLatitude() + ")";
//
//                        final JSONObject json = new JSONObject();
//                        try {
//                            json.put("id", id);
//                            json.put("point", point);
//                            json.put("time", time);
//
//                        } catch (JSONException ex) {
//                            Logger.getLogger(MTAFetchData.class.getName()).log(Level.SEVERE, null, ex);
//                        }
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
            }
        } catch (IOException ex) {
            Logger.getLogger(SocketFetchData.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        
        
//        try {
//            final URL url = new URL("http://gtfsrt.prod.obanyc.com/vehiclePositions?key=" + TOKEN);
//
//            while(true) {
//                if(System.currentTimeMillis() - lastUpdateTime < 10000) { // 10 sec
//                    try {
//                        logger.info("Sleep");
//                        Thread.sleep(10000);
//                    } catch (InterruptedException ex) {
//                        Logger.getLogger(SocketFetchData1.class.getName()).log(Level.SEVERE, null, ex);
//                    }
//                }
//
//                final FeedMessage result;
//                try {
//                    result = FeedMessage.parseFrom(url.openStream());
//                } catch (IOException ex) {
//                    Logger.getLogger(SocketFetchData1.class.getName()).log(Level.SEVERE, null, ex);
//                    continue;
//                }
//                lastUpdateTime = System.currentTimeMillis();
//
//                logger.info("Fetch " + result.getEntityCount() + " vehicles");
//                result.getEntityList().forEach((entity) -> {
//                    if(entity.hasVehicle()) {
//                        final GtfsRealtime.VehiclePosition vehicle = entity.getVehicle();
//                        final GtfsRealtime.Position pos = vehicle.getPosition();
//                        final long time = vehicle.getTimestamp() * 1000; // sec to milisecond
//                        final String id = vehicle.getVehicle().getId();
//
//                        final String point = "POINT(" + pos.getLongitude() + " " + pos.getLatitude() + ")";
//
//                        final JSONObject json = new JSONObject();
//                        try {
//                            json.put("id", id);
//                            json.put("point", point);
//                            json.put("time", time);
//
//                        } catch (JSONException ex) {
//                            Logger.getLogger(SocketFetchData1.class.getName()).log(Level.SEVERE, null, ex);
//                        }
//                        final StringBuilder response = new StringBuilder();
//                        try {
//                            customResponseFactory.treatInformation(json, response);
//                        } catch (JSONException ex) {
//                            Logger.getLogger(SocketFetchData1.class.getName()).log(Level.SEVERE, null, ex);
//                        }
//
//
//                    } else {
//                        logger.warn("Could not process following entity: " + entity.toString());
//                    }
//
//                });
//                logger.info(result.getEntityCount() + " vehicles processed !");
//
//            }
//
//        } catch (IOException ex) {
//            Logger.getLogger(SocketFetchData1.class.getName()).log(Level.SEVERE, null, ex);
//        }

    }
    
    @Override
    public int getPortNumber() {
        return -1;
    }
    
}
