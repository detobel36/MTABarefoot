package be.detobel36.mtabarefoot;

import com.bmwcarit.barefoot.matcher.MatcherSample;
import com.esri.core.geometry.Point;
import com.google.transit.realtime.GtfsRealtime;
import java.util.HashMap;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author remy
 */
public class CustomMatcherSample extends MatcherSample {
    
    private HashMap<String, String> allInformations;
    
    public CustomMatcherSample(long time, Point point) {
        super(time, point);
        allInformations = new HashMap<>();
    }
    
    public CustomMatcherSample(final GtfsRealtime.FeedEntity feed) {
        super(feed.getVehicle().getVehicle().getId(), feed.getVehicle().getTimestamp()*1000, getPointFromVehicle(feed.getVehicle()));
        
        final GtfsRealtime.VehiclePosition vehicle = feed.getVehicle();
        if(vehicle.hasTrip()) {
            final GtfsRealtime.TripDescriptor trip = vehicle.getTrip();
            if(trip.hasTripId()) {
                allInformations.put("trip_id", trip.getTripId()); 
            }
            if(trip.hasStartDate()) {
                allInformations.put("start_date", trip.getStartDate());  // Format AAAAMMJJ
            }
            if(trip.hasRouteId()) {
                allInformations.put("route_id", trip.getRouteId());
            }
            if(trip.hasDirectionId()) {
                allInformations.put("direction_id", "" + trip.getDirectionId());
            }
        }
        if(vehicle.hasPosition() && vehicle.getPosition().hasBearing()) {
            allInformations.put("route_id", "" + vehicle.getPosition().getBearing());
        }
        if(vehicle.hasStopId()) {
            allInformations.put("stop_id", "" + vehicle.getStopId());
        }
    }
    
    public CustomMatcherSample(final JSONObject json) throws JSONException {
        super(json);
    }
    
    
    private static Point getPointFromVehicle(final GtfsRealtime.VehiclePosition vehicle) {
        return new Point(vehicle.getPosition().getLongitude(), vehicle.getPosition().getLatitude());
    }

}
