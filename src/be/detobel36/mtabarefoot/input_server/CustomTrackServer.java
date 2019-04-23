package be.detobel36.mtabarefoot.input_server;

import be.detobel36.mtabarefoot.CustomMatcherKState;
import be.detobel36.mtabarefoot.CustomMatcherSample;
import be.detobel36.mtabarefoot.TemporaryMemory;
import com.bmwcarit.barefoot.matcher.Matcher;
import com.bmwcarit.barefoot.matcher.MatcherCandidate;
import com.bmwcarit.barefoot.matcher.MatcherSample;
import com.bmwcarit.barefoot.roadmap.Road;
import com.bmwcarit.barefoot.roadmap.RoadMap;
import com.bmwcarit.barefoot.roadmap.RoadPoint;
import com.bmwcarit.barefoot.roadmap.TimePriority;
import com.bmwcarit.barefoot.scheduler.StaticScheduler;
import com.bmwcarit.barefoot.scheduler.Task;
import com.bmwcarit.barefoot.spatial.Geography;
import com.bmwcarit.barefoot.spatial.SpatialOperator;
import com.bmwcarit.barefoot.topology.Dijkstra;
import com.bmwcarit.barefoot.util.AbstractServer;
import com.bmwcarit.barefoot.util.Stopwatch;
import com.google.transit.realtime.GtfsRealtime;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author remy
 */
public class CustomTrackServer extends AbstractServer {
    
    private final static Logger logger = LoggerFactory.getLogger(CustomTrackServer.class);
    private final static SpatialOperator spatial = new Geography();
    protected final MatcherResponseFactory customResponseFactory;
    
    public CustomTrackServer(final Properties properties, final RoadMap map, final TemporaryMemory.Publisher typePublisher) {
        this(properties, new MatcherResponseFactory(properties, map, typePublisher));
    }
    
    private CustomTrackServer(final Properties properties, 
            final MatcherResponseFactory responseFactory) {
        super(properties, responseFactory);
        this.customResponseFactory = responseFactory;
    }
    
    protected static class MatcherResponseFactory extends ResponseFactory {
        private final Matcher matcher;
        private final int TTL;
        private final int interval;
        private final double distance;
        private final double sensitive;
        private final TemporaryMemory<State> memory;

        public MatcherResponseFactory(final Properties properties, final RoadMap map, 
                final TemporaryMemory.Publisher publisher) {
            matcher = new Matcher(map, new Dijkstra<Road, RoadPoint>(), new TimePriority(),
                    new Geography());

            matcher.setMaxRadius(Double.parseDouble(properties.getProperty("matcher.radius.max",
                    Double.toString(matcher.getMaxRadius()))));
            matcher.setMaxDistance(Double.parseDouble(properties.getProperty("matcher.distance.max",
                    Double.toString(matcher.getMaxDistance()))));
            matcher.setLambda(Double.parseDouble(properties.getProperty("matcher.lambda",
                    Double.toString(matcher.getLambda()))));
            matcher.setSigma(Double.parseDouble(
                    properties.getProperty("matcher.sigma", Double.toString(matcher.getSigma()))));
            matcher.shortenTurns(
                    Boolean.parseBoolean(properties.getProperty("matcher.shortenturns", "true")));
            interval = Integer.parseInt(properties.getProperty("matcher.interval.min", "1000"));
            distance = Integer.parseInt(properties.getProperty("matcher.distance.min", "0"));
            sensitive = Double.parseDouble(
                    properties.getProperty("tracker.monitor.sensitive", Double.toString(0d)));
            TTL = Integer.parseInt(properties.getProperty("tracker.state.ttl", "60"));
            int port = Integer.parseInt(properties.getProperty("tracker.port", "1235"));
            memory = new TemporaryMemory<>(new TemporaryMemory.Factory<State>() {
                @Override
                public State newInstance(String id) {
                    return new State(id);
                }
            }, publisher.init(port));
            
            logger.info("tracker.state.ttl={}", TTL);
            logger.info("tracker.port={}", port);
            logger.info("tracker.monitor.sensitive={}", sensitive);
            int matcherThreads = Integer.parseInt(properties.getProperty("matcher.threads",
                    Integer.toString(Runtime.getRuntime().availableProcessors())));

            StaticScheduler.reset(matcherThreads, (long) 1E4);

            logger.info("matcher.radius.max={}", matcher.getMaxRadius());
            logger.info("matcher.distance.max={}", matcher.getMaxDistance());
            logger.info("matcher.lambda={}", matcher.getLambda());
            logger.info("matcher.sigma={}", matcher.getSigma());
            logger.info("matcher.threads={}", matcherThreads);
            logger.info("matcher.shortenturns={}", matcher.shortenTurns());
            logger.info("matcher.interval.min={}", interval);
            logger.info("matcher.distance.min={}", distance);
        }

        @Override
        public ResponseHandler response(String request) {
            return new ResponseHandler(request) {
                @Override
                protected RESULT response(final String request, final StringBuilder response) {
                    try {
                        final JSONObject json = new JSONObject(request);
                        return treatInformation(json, response);
                    } catch (RuntimeException | JSONException e) {
                        logger.error("{}", e.getMessage());
                        e.printStackTrace();
                        return RESULT.ERROR;
                    }
                }
            };
        }
        
        public RESULT treatInformation(final GtfsRealtime.FeedEntity feed, final StringBuilder response) throws JSONException {
            final CustomMatcherSample sample = new CustomMatcherSample(feed);
            return processTreatInformation(sample);
        }
        
        public RESULT treatInformation(final JSONObject json, final StringBuilder response) throws JSONException {
            if (!json.optString("id").isEmpty()) {
                if (!json.optString("time").isEmpty()
                        && !json.optString("point").isEmpty()) {

                    logger.debug("Execute for input: " + json.getString("id"));

                    final CustomMatcherSample sample = new CustomMatcherSample(json);
                    return processTreatInformation(sample);
                    
                } else {
                    String id = json.getString("id");
                    logger.debug("received state request for object {}", id);

                    State state = memory.getIfExistsLocked(id);

                    if (state != null) {
                        response.append(state.inner.toJSON().toString());
                        state.unlock();
                    } else {
                        JSONObject empty = new JSONObject();
                        empty.put("id", id);
                        response.append(empty.toString());
                    }

                    return RESULT.SUCCESS;
                }
            } else if (json.optJSONArray("roads") != null) {
                logger.debug("received road data request");

                return RESULT.SUCCESS;
            } else {
                throw new RuntimeException(
                        "JSON request faulty or incomplete: " + json);
            }
        }
        
        private RESULT processTreatInformation(final MatcherSample sample) {
            final State state = memory.getLocked(sample.id());

            if (state.inner.sample() != null) {
                if (sample.time() < state.inner.sample().time()) {
                    state.unlock();
                    logger.warn("received out of order sample");
                    return RESULT.ERROR;
                }
                if (spatial.distance(sample.point(),
                        state.inner.sample().point()) < Math.max(0, distance)) {
                    state.unlock();
                    logger.debug("received sample below distance threshold");
                    return RESULT.SUCCESS;
                }
                if ((sample.time() - state.inner.sample().time()) < Math.max(0,
                        interval)) {
                    state.unlock();
                    logger.debug("received sample below interval threshold");
                    return RESULT.SUCCESS;
                }
            }

            final AtomicReference<Set<MatcherCandidate>> vector =
                    new AtomicReference<>();
            final StaticScheduler.InlineScheduler scheduler = StaticScheduler.scheduler();
            scheduler.spawn(new Task() {
                @Override
                public void run() {
                    final Stopwatch sw = new Stopwatch();
                    sw.start();
                    vector.set(matcher.execute(state.inner.vector(),
                            state.inner.sample(), sample));
                    sw.stop();
                    logger.debug("state update of object {} processed in {} ms",
                            sample.id(), sw.ms());
                }
            });

            if (!scheduler.sync()) {
                state.unlock();
                logger.error("matcher execution error");
                return RESULT.ERROR;
            } else {
                boolean publish = true;
                final MatcherSample previousSample = state.inner.sample();
                final MatcherCandidate previousEstimate = state.inner.estimate();
                state.inner.update(vector.get(), sample);

                if (previousSample != null && previousEstimate != null) {
                    if (spatial.distance(previousSample.point(),
                            sample.point()) < sensitive
                            && previousEstimate.point().edge()
                                    .id() == state.inner.estimate().point()
                                            .edge().id()) {
                        publish = false;
                        logger.debug("unpublished update");
                    }
                }

                state.updateAndUnlock(TTL, publish);
                return RESULT.SUCCESS;
            }
        }
        
    }
    

    public static class State extends TemporaryMemory.TemporaryElement<CustomTrackServer.State> {
        public final CustomMatcherKState inner = new CustomMatcherKState();

        public State(String id) {
            super(id);
        }
    };
    
}
