package be.detobel36.mtabarefoot.publisher;

import be.detobel36.mtabarefoot.input_server.CustomTrackServer;
import be.detobel36.mtabarefoot.MTABarefoot;
import be.detobel36.mtabarefoot.TemporaryMemory;
import com.bmwcarit.barefoot.util.SourceException;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author remy
 */
public class StatePublisherPostgreSQL implements TemporaryMemory.Publisher<CustomTrackServer.State> {
    
    private final static Logger logger = LoggerFactory.getLogger(StatePublisherPostgreSQL.class);
    
    private final Pattern pattern = Pattern.compile("\\:([0-9a-zA-Z\\_\\-\\.])+ ");
    private PostgresPublisher postgresSource = null;
    private HashMap<String, Boolean> listRequest;
    
    private Long totalTime = 0l;
    private int totalRequest = 0;
    private int totalData = 0;
    
    public StatePublisherPostgreSQL(final String requestPath) {
        loadRequest(requestPath);
        loadPostgresSource();
    }
    
    public final void loadRequest(final String requestPath) {
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
    public void publish(final String id, final CustomTrackServer.State state) {
        final HashMap<String, String> pointInfos = state.inner.toHashMap();
        pointInfos.put("id", id);
        
        listRequest.entrySet().forEach((request) -> {
            String strRequest = request.getKey();
            strRequest = addValueToQuery(pointInfos, strRequest);
            if(!strRequest.equals("")) {
                if(MTABarefoot.viewQuery()) {
                    logger.info("Req :" + strRequest);
                }
                try {
                    if(request.getValue()) { // If select
                        final Long startTime = System.currentTimeMillis();
                        final ResultSet result = postgresSource.getResultSet(strRequest);
                        totalTime += (System.currentTimeMillis()-startTime);
                        ++totalRequest;
                        if(result != null) {
                            String strResult = "";

                            final int numCol = result.getMetaData().getColumnCount();
                            while (result.next()) {
                                for (int i = 1; i <= numCol; ++i) {
                                    strResult += result.getString(i) + " ";
                                }
                                strResult += ";";
                            }
                            if(MTABarefoot.viewQuery()) {
                                logger.info("Result: " + strResult);
                            }
                        }
                        

                    } else {
                        final Long startTime = System.currentTimeMillis();
                        postgresSource.execute(strRequest);
                        totalTime += (System.currentTimeMillis()-startTime);
                        ++totalRequest;
                    }
                } catch (SQLException | ClassNotFoundException ex) {
                    logger.error("Erreur avec la requête " + strRequest, ex);
                }
            }
        });
        ++totalData;
        
        if(MTABarefoot.viewQuery()) {
            logger.info(pointInfos.toString());
        }
        
    }
    
    private String addValueToQuery(final HashMap<String, String> pointInfos, String query) {
//        logger.info("Avant addValueToQuery: ");
//        logger.info("Query: " + query);
        for(final Map.Entry<String, String> infos : pointInfos.entrySet()) {
            String value = infos.getValue();
            if(infos.getKey().equalsIgnoreCase("time")) {
                value = ""+ Long.parseLong(value)/1000;
            }
            query = query.replaceAll(":" + infos.getKey() + " ", value);
//            logger.info("Set " + infos.getKey() + " - " + infos.getValue());
//            logger.info("Res: " + query);
        }
        
        if(pattern.matcher(query).find()) {
            logger.info("Requete ignoré car des valeurs n'ont pas pu être remplacée: '" + 
                    query + "'");
            logger.info("Infos: " + pointInfos.keySet());
            return "";
        }
        
        return query;
    }

    @Override
    public void delete(String id, long time) {
        // Do nothing
    }

    @Override
    public void reload() {
        final String requestPath = MTABarefoot.getRequestPath();
        loadRequest(requestPath);
        logger.info("Reload request path: " + requestPath);
        loadPostgresSource();
        logger.info("Reload Postgresql");
    }

    private void loadPostgresSource() {
        final Properties databaseProperties = MTABarefoot.getDatabaseProperties();
        String host = databaseProperties.getProperty("database.host");
        int port = Integer.parseInt(databaseProperties.getProperty("database.port", "0"));
        String database = databaseProperties.getProperty("database.name");
        String user = databaseProperties.getProperty("database.user");
        String password = databaseProperties.getProperty("database.password");
        
        if (host == null || port == 0 || database == null || user == null
                || password == null) {
            throw new SourceException("could not read database properties");
        }
        
        boolean initAgain = false;
        if(postgresSource != null) {
            postgresSource.close();
            initAgain = true;
        }
        
        postgresSource = new PostgresPublisher(host, port, database, user, password);
        if(initAgain) {
            logger.info("Open PostgresSource");
            postgresSource.open();
        }
    }

    @Override
    public void printStats() {
        logger.info("Moyenne d'une req SQL: " + (totalTime/totalRequest) + " ms");
        logger.info("Moyenne par data: " + (totalTime/totalData) + " ms");
    }
    
}
