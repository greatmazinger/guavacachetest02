package net.veroy.guava.benchmark.cache;


import net.veroy.guava.benchmark.cache.ObjectRecord;
import net.veroy.guava.benchmark.cache.UpdateRecord;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
// TODO import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class GuavaCacheTest02 {
    private static Cache<Integer, ObjectRecord> cache;
    private static Connection conn;

    public static void main(String[] args) {
        // TODO Hard-coded filepath for now TODO
        cache = CacheBuilder.newBuilder()
            .maximumSize(140000000)
            .build();
        conn = null;
        Statement stmt = null;
        try {
            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection("jdbc:sqlite:guava_cache.db");
            stmt = conn.createStatement();
            String sql = "DROP TABLE IF EXISTS HEAP";
            stmt.executeUpdate(sql);
            sql = "CREATE TABLE HEAP" +
                  "( objid INT PRIMARY KEY  NOT NULL," +
                  "  age            INT     NOT NULL, " +
                  "  alloctime      INT     NOT NULL, " +
                  "  deathtime      INT, " +
                  "  type           CHAR(4096) )";
            stmt.executeUpdate(sql);
            stmt.close();
            String filename = (args.length > 0) ? args[0] : "";
            // TODO hardcoded filename
            processInput( filename );
            conn.close();
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
        }
        System.out.println("Database ran successfully");
    }

    private static ObjectRecord getFromDB( int objId ) throws SQLException {
        ObjectRecord objrec = new ObjectRecord();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery( String.format("SELECT * FROM HEAP WHERE objid=%d;", objId) );
        if (rs.next()) {
            objrec.set_objId( rs.getInt("objid") );
            objrec.set_age( rs.getInt("age") );
            objrec.set_allocTime( rs.getInt("alloctime") );
            objrec.set_deathTime( rs.getInt("deathtime") );
            objrec.set_type( rs.getString("type") );
        } else {
            objrec.set_objId( objId );
        }
        return objrec;
    }

    private static boolean putIntoDB( ObjectRecord newrec ) throws SQLException {
        Statement stmt = conn.createStatement();
        int objId = newrec.get_objId();
        int age = newrec.get_age();
        int allocTime = newrec.get_allocTime();
        int deathTime = newrec.get_deathTime();
        String type = newrec.get_type();
        stmt.executeUpdate( String.format( "INSERT OR REPLACE INTO HEAP " +
                                           "(objid,age,alloctime,deathtime,type) " +
                                           " VALUES (%d,%d,%d,%d,'%s');",
                                           objId, age, allocTime, deathTime, type ) );
        return true;
    }

    private static void processInput( String filename ) throws SQLException, ExecutionException {
        try {
            int i = 0;
            String line;
            ArrayList<UpdateRecord> updateList = new ArrayList<UpdateRecord>();
            try (
                  InputStream fis = (!filename.isEmpty()) ? new FileInputStream( filename )
                                                          : System.in;
                  InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
                  BufferedReader bufreader = new BufferedReader(isr);
            ) {
                int timeByMethod = 0;
                while ((line = bufreader.readLine()) != null) {
                    // Deal with the line
                    String[] fields = line.split(" ");
                    if (isAllocation(fields[0])) {
                        ObjectRecord rec = parseAllocation( fields, timeByMethod );
                        try {
							putIntoDB( rec );
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
                            continue;
						}
                        int objId = rec.get_objId();
                        cache.put( objId, rec );
                    }
                    else if (isUpdate(fields[0])) {
                        UpdateRecord rec = parseUpdate( fields, timeByMethod );
                        updateList.add(rec);
                    }

                    i += 1;
                    if (i % 10000 == 1) {
                        System.out.print(".");
                    } 
                }
            }
            System.out.println("");
        } catch (IOException e) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
        }
    }

    private static boolean isUpdate(String op) {
        return op.equals("U");
    }

    private static boolean isAllocation( String op ) {
        return (op.equals("A") || op.equals("N") || op.equals("P") || op.equals("I"));
    }

    private static ObjectRecord parseAllocation( String[] fields, int timeByMethod ) {
        // System.out.println("[" + fields[0] + "]");
        int objId = Integer.parseInt( fields[1], 16 );
        String type = fields[3];
        // UNUSED right now:
        // int size = Integer.parseInt( fields[2], 16 );
        // int site = Integer.parseInt( fields[4], 16 );
        // int length = Integer.parseInt( fields[5], 16 );
        // int threadId = Integer.parseInt( fields[6], 16 );
        return new ObjectRecord( 0, // Autogenerated by database
                                 objId,
                                 0, // Unknown at this point
                                 timeByMethod,
                                 0, // Unknown at this point
                                 type );
    }

    private static UpdateRecord parseUpdate( String[] fields, int timeByMethod ) {
        int oldTgtId = Integer.parseInt( fields[1], 16 );
        int objId = Integer.parseInt( fields[2], 16 );
        int newTgtId = Integer.parseInt( fields[3], 16 );
        int fieldId = 0;
        try {
            fieldId = Integer.parseInt( fields[4], 16 );
        }
        catch ( Exception e ) {
            try {
                System.out.println( String.format("parseInt failed: %d -> %s", objId, fields[4]) );
                BigInteger tmp = new BigInteger( fields[4], 16 );
                fieldId = tmp.intValue();
            }
            catch ( Exception e2 ) {
                System.err.println( e2.getClass().getName() + ": " + e2.getMessage() );
                System.exit(0);
            }
        }
        int threadId = Integer.parseInt( fields[5], 16 );
        return new UpdateRecord( objId,
                                 oldTgtId,
                                 newTgtId,
                                 fieldId,
                                 threadId,
                                 timeByMethod );
    }
}
