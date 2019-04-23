package be.detobel36.mtabarefoot;

import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author remy
 */
public class ScanIn extends Thread {
    
    private static final Logger logger = LoggerFactory.getLogger(ScanIn.class);
    
    public ScanIn() {
        super("Scan");
        start();
    }
    
    @Override
    public void run() {
        while(true){
            final Scanner scanIn = new Scanner(System.console().readLine());
            final String cmd = scanIn.nextLine();
            
            switch(cmd.toLowerCase()) {
                case "reload_output":
                    MTABarefoot.reloadOutput();
                    break;
                
                case "view_query":
                    MTABarefoot.switchViewQuery();
                    break;
                    
                case "help":
                default:
                    logger.info("Usage: help|reload_output|view_query");
                    break;
            }
        }
    }
    
}
