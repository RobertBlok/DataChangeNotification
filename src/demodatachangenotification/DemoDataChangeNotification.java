/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package demodatachangenotification;

import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.dcn.DatabaseChangeRegistration;

/**
 *
 * @author Robert Blok <robert@basetide.com>
 */
public class DemoDataChangeNotification {

  /**
   * @param args the command line arguments
   */
  public static void main( String[] args ) {
    ConnectionClass conn = new ConnectionClass( "jdbc:oracle:thin:@mbpvmora01:1521/TEST", "ROBERT", "TEST" );
    Properties prop = new Properties();
    DCNListener list  = new DCNListener();
    
    // Ask the server to send the ROWIDs as part of the DCN events (small performance cost):
    prop.setProperty( OracleConnection.DCN_NOTIFY_ROWIDS, "true" );

    //Set the DCN_QUERY_CHANGE_NOTIFICATION option for query registration with finer granularity.
    prop.setProperty(OracleConnection.DCN_QUERY_CHANGE_NOTIFICATION,"true");

    
    try {
      conn.OpenConnection( "oracle.jdbc.driver.OracleDriver" );

      DatabaseChangeRegistration dcr = conn.connection.registerDatabaseChangeNotification(prop);
        
      System.out.println( "Start waiting for event..." );

      conn.WaitForSignal();
      
      conn.CloseConnection();

    } catch ( SQLException ex ) {
      Logger.getLogger( ConnectionClass.class.getName() ).log( Level.SEVERE, null, ex );
    }
  }
  
}
