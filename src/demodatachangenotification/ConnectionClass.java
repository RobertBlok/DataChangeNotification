/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package demodatachangenotification;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleStatement;
import oracle.jdbc.dcn.DatabaseChangeRegistration;

/**
 *
 * @author Robert Blok <robert@basetide.com>
 */
public class ConnectionClass {
  String connectString;
  String username;
  String password;
  OracleConnection connection = null;
  DatabaseChangeRegistration dcr;
    
  String ExecuteSingleColumnQuery( String query ) {
    String resultData = null;
   
    if ( query != null ) {
      try {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery( query );
        
        if ( rs.next() ) {
          resultData = rs.getString( 1 );
        }

        rs.close();
        stmt.close();
      } catch (SQLException ex) {
        Logger.getLogger(ConnectionClass.class.getName()).log(Level.SEVERE, "Cannot execute statement", ex);
      }
    }
    
    return resultData;
  } /* ExecuteSingleColumnQuery */
  
  void WaitForSignal() throws SQLException {
    Properties prop = new Properties();

    /*
     * Ask the server to send the ROWIDs as part of the DCN events (small performance cost):
     */
    prop.setProperty( OracleConnection.DCN_NOTIFY_ROWIDS, "true" );

    /*
     * Activate "query" change notification as opposed to the "table" change notification:
     */
    prop.setProperty( OracleConnection.DCN_QUERY_CHANGE_NOTIFICATION, "true" );

    /*
     * The following operation does a roundtrip to the database to create a new registration for DCN. It sends the client address
     * (ip address and port) that the server will use to connect to the client and send the notification when necessary. Note that
     * for now the registration is empty (we haven't registered any table). This also opens a new thread in the drivers. This thread
     * will be dedicated to DCN (accept connection to the server and dispatch the events to the listeners).
     */
   
    dcr = connection.registerDatabaseChangeNotification( prop );

    /*
     * Create listener and add it to the connection
     */
    DCNListener listener = new DCNListener();
    dcr.addListener( listener );

    try (
      /*
       * Add objects to the listener
       */
      Statement stmt = connection.createStatement()) {
      String query = "select * from TEST";
      System.out.println( "Register the following query: " + query );
      ( (OracleStatement)stmt ).setDatabaseChangeRegistration( dcr );

      try (ResultSet rs = stmt.executeQuery( query )) {
        /*
         * Collecting the data
         */
        while ( rs.next() )
        {}

        /*
         * Purely informational
         */
        String[] tableNames = dcr.getTables();
        for ( String tableName : tableNames ) {
          System.out.println( tableName + " is part of the registration." );
        }
        
        
      }
      
      synchronized( this ) {
      
        System.out.println( "Starting the wait inside synchronized code" );
        try {
          this.wait();
        } catch ( InterruptedException ie ) {
          System.out.println( "Interrupt received" );
        }
        
      }
    }
  }

  void OpenConnection( String driver ) {

    try {
      Class.forName( driver );
    } catch (ClassNotFoundException ex) {
      Logger.getLogger( ConnectionClass.class.getName()).log(Level.SEVERE, "Driver not found", ex);
      System.exit( 1 );
    }
    
    try {
      connection = ( OracleConnection ) DriverManager.getConnection( connectString, username, password );
      
      
    } catch (SQLException ex) {
      Logger.getLogger( ConnectionClass.class.getName()).log(Level.SEVERE, "Error connecting to the database", ex );
      System.exit( 1 );
    }
    

  } /* OpenConnection */

    
  void CloseConnection() {
    
    try {
      if( connection != null ) {
        connection.unregisterDatabaseChangeNotification( dcr );
      }

      
      connection.close();
    } catch (SQLException ex) {
      Logger.getLogger( ConnectionClass.class.getName()).log(Level.SEVERE, "Problem closing the database connection", ex);
      System.exit( 1 );
    }
  } /* CloseConnection */
  
  /*
   * Constructor
   */
  ConnectionClass( String connectString, String username, String password ) {
    this.connectString = connectString;
    this.username = username;
    this.password = password;
  } /* Constructor */
  
}
