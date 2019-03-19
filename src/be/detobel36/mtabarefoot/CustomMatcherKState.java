package be.detobel36.mtabarefoot;

import com.bmwcarit.barefoot.matcher.MatcherCandidate;
import com.bmwcarit.barefoot.matcher.MatcherKState;
import com.bmwcarit.barefoot.matcher.MatcherSample;
import com.bmwcarit.barefoot.matcher.MatcherTransition;
import com.bmwcarit.barefoot.roadmap.Road;
import com.bmwcarit.barefoot.roadmap.RoadPoint;
import com.bmwcarit.barefoot.roadmap.Route;
import com.esri.core.geometry.GeometryEngine;
import org.json.JSONException;
import org.json.JSONObject;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.WktExportFlags;
import java.util.List;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author remy
 */
public class CustomMatcherKState extends MatcherKState {
    
    private final static Logger logger = LoggerFactory.getLogger(CustomMatcherKState.class);
    
    private Polyline monitorRoute(final MatcherCandidate candidate) {
        final Polyline routes = new Polyline();
        MatcherCandidate predecessor = candidate;
        while (predecessor != null) {
            final MatcherTransition transition = predecessor.transition();
            if (transition != null) {
                final Polyline route = transition.route().geometry();
                routes.startPath(route.getPoint(0));
                for (int i = 1; i < route.getPointCount(); ++i) {
                    routes.lineTo(route.getPoint(i));
                }
            }
            predecessor = predecessor.predecessor();
        }
        return routes;
    }
    
    @Override
    public JSONObject toMonitorJSON() throws JSONException {
        final JSONObject json = new JSONObject();
        
        // For time we need to use sample. Estimatated data doesn't have time information
        final List<MatcherSample> listSamples = samples();
        final MatcherSample lastSample = listSamples.get(listSamples.size()-1);
        final MatcherCandidate estimatePoint = estimate();
        
        json.put("time", lastSample.time());
        json.put("point", GeometryEngine.geometryToWkt(estimatePoint.point().geometry(),
                WktExportFlags.wktExportPoint));
        final Polyline routes = monitorRoute(estimatePoint);
        if (routes.getPathCount() > 0) {
            json.put("route",
                    GeometryEngine.geometryToWkt(routes, WktExportFlags.wktExportMultiLineString));
        }
        
        final JSONArray candidates = new JSONArray();
        for (final MatcherCandidate candidate : vector()) {
            JSONObject jsoncandidate = new JSONObject();
            jsoncandidate.put("point", GeometryEngine.geometryToWkt(candidate.point().geometry(),
                    WktExportFlags.wktExportPoint));
            jsoncandidate.put("prob",
                    Double.isInfinite(candidate.filtprob()) ? "Infinity" : candidate.filtprob());
            
            final Polyline candidateRoute = monitorRoute(candidate);
            if (candidateRoute.getPathCount() > 0) {
                jsoncandidate.put("route", GeometryEngine.geometryToWkt(candidateRoute,
                        WktExportFlags.wktExportMultiLineString));
            }
            candidates.put(jsoncandidate);
        }
        json.put("candidates", candidates);
        
        if(listSamples.size() > 1) {
            final MatcherSample beforeLast = listSamples.get(listSamples.size()-2);
            final long timeDifference = lastSample.time() - beforeLast.time();
            final Route estimateRoute = estimatePoint.transition().route();
            final double distance = estimateRoute.length();
            final double speedMs = distance/(timeDifference); // m/ms
            final double speed = distance/(timeDifference/1000); // m/s
            final double speedKmH = 3.6*speed; // km/h
            
            json.put("distance", distance);
            json.put("timeDiff", timeDifference); // In milisecond
            json.put("speed", speedKmH); // In km/h
            
            final JSONArray jsonRoad = new JSONArray();
            final JSONArray jsonPoint = new JSONArray();
            
            if(estimateRoute.size() == 1) { // Tester de manière pratique ce cas de figure
                final RoadPoint source = estimateRoute.source();
                final RoadPoint target = estimateRoute.target();
                
                // Start
                final JSONObject roadStartJson = source.edge().toJSON();
                roadStartJson.put("timeStart", (long) beforeLast.time()/1000);
                roadStartJson.put("fractionStart", source.fraction());
                roadStartJson.put("distance", (target.fraction() - source.fraction()) * source.edge().length());
                
                String startPointCoord = GeometryEngine.geometryToWkt(source.geometry(),
                        WktExportFlags.wktExportPoint);
                startPointCoord += "@" + ((long) beforeLast.time() / 1000);
                jsonPoint.put(startPointCoord);
                
                // End
                roadStartJson.put("timeEnd", (long) lastSample.time()/1000);
                roadStartJson.put("fractionEnd", target.fraction());
                jsonRoad.put(roadStartJson);
                
                String endPointCoord = GeometryEngine.geometryToWkt(target.geometry(),
                        WktExportFlags.wktExportPoint);
                endPointCoord += "@" + ((long) lastSample.time()/ 1000);
                jsonPoint.put(endPointCoord);
                
            } else {
                long prevTime = beforeLast.time();
//                double calculateDistance = 0;
                
                // Start
                final RoadPoint source = estimateRoute.source();
                final Road endSource = estimateRoute.get(0);
                final double startSegmentLenght = (1 - source.fraction()) * endSource.length();
                final JSONObject roadStartJson = endSource.toJSON();
                roadStartJson.put("timeStart", (long) prevTime/1000);
                roadStartJson.put("fractionStart", source.fraction());
                roadStartJson.put("fractionEnd", 1);
                roadStartJson.put("distance", startSegmentLenght);
//                calculateDistance += startSegmentLenght;
                
                String startPointCoord = GeometryEngine.geometryToWkt(source.geometry(),
                        WktExportFlags.wktExportPoint);
                startPointCoord += "@" + ((long) prevTime/1000);
                jsonPoint.put(startPointCoord);
                
                prevTime = prevTime + (long) (startSegmentLenght / speedMs);
                roadStartJson.put("timeEnd", ((long) prevTime/1000));
                jsonRoad.put(roadStartJson);
                
                String endStartPointCoord = GeometryEngine.geometryToWkt(
                        endSource.geometry().getPoint(endSource.geometry().getPointCount()-1),
                        WktExportFlags.wktExportPoint);
                endStartPointCoord += "@" + ((long) prevTime/1000);
                jsonPoint.put(endStartPointCoord);
                
                
                // Other point
                if(estimateRoute.size() > 2) {
                    for(int index = 1; index < estimateRoute.size()-1; ++index) {
                        // Vérifier que la "currentRoad.length()" prend bien en compte le fait que l'on ne fait peut-être pas toute la route
                        // Je pense que c'est bon pour les routes au milieu mais qu'il faut faire des exceptions pour la premire et la dernière
                        final Road currentRoad = estimateRoute.get(index);
                        final JSONObject roadJson = currentRoad.toJSON();
                        
                        roadJson.put("fractionStart", 0);
                        roadJson.put("fractionEnd", 1);
                        roadJson.put("distance", currentRoad.length());
//                        calculateDistance += currentRoad.length();
                        roadJson.put("timeStart", (long) prevTime/1000);
                        
                        prevTime = prevTime + (long) (currentRoad.length() / speedMs);
                        roadJson.put("timeEnd", (long) prevTime/1000);
                        jsonRoad.put(roadJson);
                        
                        String endCurrentPointCoord = GeometryEngine.geometryToWkt(
                                currentRoad.geometry().getPoint(currentRoad.geometry().getPointCount()-1),
                                WktExportFlags.wktExportPoint);
                        endCurrentPointCoord += "@" + ((long) prevTime/1000);
                        jsonPoint.put(endCurrentPointCoord);
                    }
                }
                
                // End point
                final RoadPoint target = estimateRoute.target();
                final Road endRoad = estimateRoute.get(estimateRoute.size()-1);
                final double endSegmentLenght = target.fraction() * endRoad.length();
                final JSONObject roadEndJson = endRoad.toJSON();
                roadEndJson.put("timeStart", (long) prevTime/1000);
                roadEndJson.put("fractionStart", 0);
                roadEndJson.put("fractionEnd", target.fraction());
                roadEndJson.put("distance", endSegmentLenght);
//                calculateDistance += endSegmentLenght;
                
                // Potentiellement un doublon !
                String endPointCoord = GeometryEngine.geometryToWkt(endRoad.geometry().getPoint(0),
                        WktExportFlags.wktExportPoint);
                endPointCoord += "@" + ((long) prevTime/1000);
                jsonPoint.put(endPointCoord);
                
                prevTime = prevTime + (long) (endSegmentLenght / speedMs);
                roadEndJson.put("timeEndCalculated", (long) prevTime/1000);
                roadEndJson.put("timeEnd", ((long) lastSample.time()/1000));
                jsonRoad.put(roadEndJson);
                
                String endEndPointCoord = GeometryEngine.geometryToWkt(target.geometry(),
                        WktExportFlags.wktExportPoint);
                endEndPointCoord += "@" + ((long) lastSample.time()/1000);
                jsonPoint.put(endEndPointCoord);
                
                
//                json.put("distanceCalculee", calculateDistance);
            }
            json.put("timeRoad", jsonRoad);
            json.put("timeCoordinate", jsonPoint);
        }
        
        return json;
    }
    
}
