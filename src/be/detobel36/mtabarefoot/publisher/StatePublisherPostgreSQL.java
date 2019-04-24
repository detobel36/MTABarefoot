package be.detobel36.mtabarefoot.publisher;

import be.detobel36.mtabarefoot.input_server.CustomTrackServer;
import be.detobel36.mtabarefoot.MTABarefoot;
import be.detobel36.mtabarefoot.TemporaryMemory;
import com.bmwcarit.barefoot.util.SourceException;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author remy
 */
public class StatePublisherPostgreSQL implements TemporaryMemory.Publisher<CustomTrackServer.State> {
    
    private final static Logger logger = LoggerFactory.getLogger(StatePublisherPostgreSQL.class);
    
    private PostgresPublisher postgresSource;
    private HashMap<String, Boolean> listRequest;
    private ArrayList<String> listQueryToExecute = new ArrayList<String>();
    
    
    public StatePublisherPostgreSQL(final Properties databaseProperties, 
            final String requestPath) {
        String host = databaseProperties.getProperty("database.host");
        int port = Integer.parseInt(databaseProperties.getProperty("database.port", "0"));
        String database = databaseProperties.getProperty("database.name");
        String user = databaseProperties.getProperty("database.user");
        String password = databaseProperties.getProperty("database.password");
        
        if (host == null || port == 0 || database == null || user == null
                || password == null) {
            throw new SourceException("could not read database properties");
        }
        
        loadRequest(requestPath);
        
        postgresSource = new PostgresPublisher(host, port, database, user, password);
    }
    
    public void loadRequest(final String requestPath) {
        // Stock the request + true if we need to wait some result
        listRequest = new HashMap<String, Boolean>();
        // Variable temporaire
        String currentStr = "";
        
        try (BufferedReader br = Files.newBufferedReader(Paths.get(requestPath))) {
            // read line by line
            String line;
            while ((line = br.readLine()) != null) {
                final int indexBrace = line.indexOf(";");
                if(indexBrace != -1) {
                    currentStr += line.substring(0, indexBrace+1);
                    if(!currentStr.trim().equalsIgnoreCase("")) {
                        final boolean isSelect = currentStr.split(" ")[0].trim()
                                                        .equalsIgnoreCase("SELECT");
                        listRequest.put(currentStr, isSelect);
                    }
                    currentStr = line.substring(indexBrace+1) + " ";
                    
                } else {
                    currentStr += line + " ";
                }
            }
            if(!currentStr.trim().equalsIgnoreCase("")) {
                final boolean isSelect = currentStr.split(" ")[0].trim()
                                                .equalsIgnoreCase("SELECT");
                listRequest.put(currentStr, isSelect);
            }
            
            logger.info("Loaded request: ");
            listRequest.keySet().forEach((request) -> {
                logger.info("> " + request);
            });
            logger.info("----");
            
        } catch (IOException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }
    

    @Override
    public StatePublisherPostgreSQL init(int port) {
        postgresSource.open();
        return this;
    }
    
    @Override
    public void publish(String id, CustomTrackServer.State state) {
        final HashMap<String, String> pointInfos = state.inner.toHashMap();
        pointInfos.put("id", id);
        
        listRequest.entrySet().forEach((request) -> {
            String strRequest = request.getKey();
            strRequest = addValueToQuery(pointInfos, strRequest);
            if(!strRequest.equals("")) {
                if(MTABarefoot.viewQuery()) {
                    logger.info("Req :" + strRequest);
                }
                
//                listQueryToExecute.add(strRequest);
                
//                try {
//                    if(request.getValue()) { // If select
//                        final ResultSet result = postgresSource.getResultSet(strRequest);
//                        String strResult = "";
//
//                        final int numCol = result.getMetaData().getColumnCount();
//                        while (result.next()) {
//                            for (int i = 1; i <= numCol; ++i) {
//                                strResult += result.getString(i) + " ";
//                            }
//                            strResult += ";";
//                        }
//                        if(MTABarefoot.viewQuery()) {
//                            logger.info("Result: " + strResult);
//                        }
//                        
//
//                    } else {
//                        postgresSource.execute(strRequest);
//                    }
//                } catch (SQLException | ClassNotFoundException ex) {
//                    logger.error("Erreur avec la requête " + strRequest, ex);
//                }
            }
        });
        
        if(MTABarefoot.viewQuery()) {
            logger.info(pointInfos.toString());
        }
        
    }
    
    private String addValueToQuery(final HashMap<String, String> pointInfos, String query) {
//        logger.info("Avant addValueToQuery: ");
//        logger.info("Query: " + query);
        for(Map.Entry<String, String> infos : pointInfos.entrySet()) {
            String value = infos.getValue();
            if(infos.getKey().equalsIgnoreCase("time")) {
                value = ""+ Long.parseLong(value)/1000;
            }
            query = query.replaceAll(":" + infos.getKey() + " ", value);
//            logger.info("Set " + infos.getKey() + " - " + infos.getValue());
//            logger.info("Res: " + query);
        }
        if(query.contains(":")) {
            logger.info("Requete ignoré car des valeurs n'ont pas pu être remplacée: '" + 
                    query + "'");
            return "";
        }
        
        return query;
    }

    @Override
    public void delete(String id, long time) {
//            try {
//                final JSONObject json = new JSONObject();
//                json.put("id", id);
//                json.put("time", time);
//                queue.put(json.toString());
//                logger.debug("delete object {}", id);
//            } catch (InterruptedException | JSONException e) {
//                logger.error("delete failed: {}", e.getMessage());
//                e.printStackTrace();
//            }
    }

    @Override
    public void reload() {
        final String requestPath = MTABarefoot.getRequestPath();
        loadRequest(requestPath);
        logger.info("Reload request path: " + requestPath);
    }
    
    public void commit() {
        logger.info(">>>>>>>>>>>>>>>>");
        logger.info("");
        logger.info("<<<<<<<<<<<<<<<<");
//        final ArrayList<String> listQuery = (ArrayList<String>) listQueryToExecute.clone();
//        listQueryToExecute.clear();
//        
//        new Thread() {
//            @Override
//            public void run() {
//                logger.info("Begin commit !!!!!!!!!!!!!!!!!!!!");
//                final Long startCommit = System.currentTimeMillis();
//                listQuery.forEach((query) -> {
//                    try {
//                        postgresSource.execute(query);
//                    } catch (SQLException | ClassNotFoundException ex) {
//                        java.util.logging.Logger.getLogger(StatePublisherPostgreSQL.class.getName()).log(Level.SEVERE, null, ex);
//                    }
//                });
//                try {
//                    postgresSource.commit();
//                } catch (SQLException ex) {
//                    logger.error("Erreur commit: " + ex.getMessage());
//                }
//                logger.info("------------- End start commit: " + (System.currentTimeMillis()-startCommit) + " ms");
//            }
//        }.start();
        
    }
    
}
