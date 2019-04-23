package be.detobel36.mtabarefoot.publisher;

import be.detobel36.mtabarefoot.input_server.CustomTrackServer;
import be.detobel36.mtabarefoot.TemporaryMemory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 *
 * @author remy
 */
public class StatePublisherExternalTcp extends Thread implements TemporaryMemory.Publisher<CustomTrackServer.State> {
    
    private final static Logger logger = LoggerFactory.getLogger(StatePublisherExternalTcp.class);
    
    private final BlockingQueue<String> queue = new LinkedBlockingDeque<>();
    private ZMQ.Context context = null;
    private ZMQ.Socket socket = null;

    @Override
    public StatePublisherExternalTcp init(int port) {
        context = ZMQ.context(1);
        socket = context.socket(ZMQ.PUB);
        socket.bind("tcp://*:" + port);
        this.setDaemon(true);
        this.start();
        
        return this;
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

    @Override
    public void reload() { }
    
}
