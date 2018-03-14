package demodatachangenotification;


import oracle.jdbc.dcn.DatabaseChangeEvent;
import oracle.jdbc.dcn.DatabaseChangeListener;

/**
 *
 * @author Robert Blok <robert@basetide.com>
 */
public class DCNListener implements DatabaseChangeListener {

  DCNListener() {
    //throw new UnsupportedOperationException( "Not supported yet." );
  }

  @Override
  public void onDatabaseChangeNotification( DatabaseChangeEvent e )
  {
    Thread t = Thread.currentThread();
    System.out.println( "DCNDemoListener: got an event ( "+this+" running on thread " + t + " )" );
    System.out.println( e.toString() );
  }

}
