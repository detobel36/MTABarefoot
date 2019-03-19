package be.detobel36.mtabarefoot;

import com.bmwcarit.barefoot.matcher.Matcher;
import com.bmwcarit.barefoot.matcher.MatcherCandidate;
import com.bmwcarit.barefoot.matcher.MatcherKState;
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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 *
 * @author remy
 */
public class CustomTrackServer extends AbstractServer {
    
    private final static Logger logger = LoggerFactory.getLogger(CustomTrackServer.class);
    private final static SpatialOperator spatial = new Geography();
    private final RoadMap map;
    protected final MatcherResponseFactory customResponseFactory;
    
    public CustomTrackServer(final Properties properties, final RoadMap map) {
        this(properties, map, new MatcherResponseFactory(properties, map));
    }
    
    public CustomTrackServer(final Properties properties, final RoadMap map, 
            final MatcherResponseFactory responseFactory) {
        super(properties, responseFactory);
        this.customResponseFactory = responseFactory;
        this.map = map;
    }
    
    protected static class MatcherResponseFactory extends ResponseFactory {
        private final Matcher matcher;
        private final int TTL;
        private final int interval;
        private final double distance;
        private final double sensitive;
        private final TemporaryMemory<State> memory;

        public MatcherResponseFactory(Properties properties, RoadMap map) {
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
            }, new StatePublisher(port));

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
        
        public RESULT treatInformation(final JSONObject json, final StringBuilder response) throws JSONException {
            if (!json.optString("id").isEmpty()) {
                if (!json.optString("time").isEmpty()
                        && !json.optString("point").isEmpty()) {

                    logger.debug("Execute for input: " + json.getString("id"));

                    final MatcherSample sample = new MatcherSample(json);
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
    }

    private static class State extends TemporaryMemory.TemporaryElement<CustomTrackServer.State> {
        final MatcherKState inner = new CustomMatcherKState();

        public State(String id) {
            super(id);
        }
    };
    
    private static class StatePublisher extends Thread implements TemporaryMemory.Publisher<CustomTrackServer.State> {
        private final BlockingQueue<String> queue = new LinkedBlockingDeque<>();
        private ZMQ.Context context = null;
        private ZMQ.Socket socket = null;

        public StatePublisher(int port) {
            context = ZMQ.context(1);
            socket = context.socket(ZMQ.PUB);
            socket.bind("tcp://*:" + port);
            this.setDaemon(true);
            this.start();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    final String message = queue.take();
                    socket.send(message);
                } catch (InterruptedException e) {
                    logger.warn("state publisher interrupted");
                    return;
                }
            }
        }

        @Override
        public void publish(String id, CustomTrackServer.State state) {
            try {
                final JSONObject json = state.inner.toMonitorJSON();
                json.put("id", id);
                queue.put(json.toString());
            } catch (InterruptedException | JSONException e) {
                logger.error("update failed: {}", e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        public void delete(String id, long time) {
            try {
                final JSONObject json = new JSONObject();
                json.put("id", id);
                json.put("time", time);
                queue.put(json.toString());
                logger.debug("delete object {}", id);
            } catch (InterruptedException | JSONException e) {
                logger.error("delete failed: {}", e.getMessage());
                e.printStackTrace();
            }
        }
    }
    
}
