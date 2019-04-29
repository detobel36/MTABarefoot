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
    
    private final HashMap<String, String> allInformations = new HashMap<>();
    
    public CustomMatcherSample(long time, Point point) {
        super(time, point);
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
            allInformations.put("bearing", "" + vehicle.getPosition().getBearing());
        }
        if(vehicle.hasStopId()) {
            allInformations.put("stop_id", "" + vehicle.getStopId());
        }
    }
    
    public HashMap<String, String> getAllInformations() {
        return allInformations;
    }
    
    public CustomMatcherSample(final JSONObject json) throws JSONException {
        super(json);
        if(json.has("trip_id")) {
            allInformations.put("trip_id", json.getString("trip_id")); 
        }
        if(json.has("start_date")) {
            allInformations.put("start_date", json.getString("start_date")); 
        }
        if(json.has("route_id")) {
            allInformations.put("route_id", json.getString("route_id")); 
        }
        if(json.has("direction_id")) {
            allInformations.put("direction_id", json.getString("direction_id")); 
        }
        if(json.has("bearing")) {
            allInformations.put("bearing", json.getString("bearing")); 
        }
        if(json.has("stop_id")) {
            allInformations.put("stop_id", json.getString("stop_id")); 
        }
    }
    
    
    private static Point getPointFromVehicle(final GtfsRealtime.VehiclePosition vehicle) {
        return new Point(vehicle.getPosition().getLongitude(), vehicle.getPosition().getLatitude());
    }

}
