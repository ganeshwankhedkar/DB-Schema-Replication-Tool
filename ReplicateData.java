package com.hashMapData;

import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.tcs.nrms.constants.TradeConstants;
import com.tcs.nrms.util.ConfigReader;
import com.tcs.nrms.util.Logging;

public class ReplicateData {

	public Connection hSqlConn  = null, prodServerConn = null; 
	public List<String> listTableNames = null;
	public static String moduleName = null;
	public static String Data = null;
	
	static{
		System.setProperty("KEYPATH", "RESOURCEPATH");
		System.setProperty("MODULEFILENAME", ConfigReader.getProperty("CLEAR_DB_TABLE_LOGFILE"));
		String path = ConfigReader.getProperty("RESOURCEPATH");
		System.out.println(path);
		ConfigReader.loadPropertyFile(path + "replication_connections.properties");
		ConfigReader.loadPropertyFile(path + "nrmsConfig.properties");
	}
	
	private ReplicateData(){
		init();
	}
	private void init(){
		try {
			Class.forName(TradeConstants.DRIVERNAME);
			//Class.forName("org.hsqldb.jdbc.JDBCDriver");
			if(hSqlConn == null){
				String con = ConfigReader.getProperty("DESTINATION_SCHEMA");
				hSqlConn = DriverManager.getConnection(TradeConstants.CONNSTR_DESTINATION_SERVER, TradeConstants.HUSER, TradeConstants.HPASSWORD);
				//hSqlConn = DriverManager.getConnection(ConfigReader.getProperty("DESTINATION_SCHEMA"), "SA", "");
				Logging.info("Connected to HSqlConn successfully !!!!!");
			}
			if(prodServerConn == null){
				prodServerConn = DriverManager.getConnection(TradeConstants.CONNSTR_SOURCE_SERVER, TradeConstants.HUSER, TradeConstants.HPASSWORD);
				//prodServerConn = DriverManager.getConnection(ConfigReader.getProperty("SOURCE_SCHEMA"), "SA", "");
				Logging.info("Connected to prodServerConn successfully !!!!!");
			}
		} catch (SQLException e) {
			Logging.error("Failed to retrieve data !!", e);
		} catch (ClassNotFoundException e) {
			Logging.error("Failed to load the Driver !!", e);
		}
	}
	private List<String> getTableNames(){
		Statement stmt = null;
		ResultSet resTbl = null;
		listTableNames = new ArrayList<String>();
		try {
			stmt = prodServerConn.createStatement();
			if(moduleName.equals("NORMAL")){
				resTbl = stmt.executeQuery("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES where table_schema = 'PUBLIC'");				
			} else {
				resTbl = stmt.executeQuery("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES where table_schema = 'PUBLIC' and TABLE_NAME LIKE '%"+moduleName+"%'");
			}				
			while(resTbl.next()){
				listTableNames.add(resTbl.getString("TABLE_NAME"));
			}
		} catch (SQLException e) {
			Logging.error("No tables to retrieve from current Database", e);
		}
		return listTableNames;
	}
	
	private boolean iterateDBInfo(){
		ResultSet rs = null;
		String queryStr = null;
		int columnCount = 0;
		boolean isSuccess = false;
		try {
			DatabaseMetaData dbMetaData = prodServerConn.getMetaData();
			List<String> list = getTableNames();
			Iterator itr = list.iterator();
			while(itr.hasNext()){
				String TABLE_NAME = (String) itr.next();
				System.out.println("TABLE_NAME == "+TABLE_NAME);
				rs = dbMetaData.getColumns(null, null,TABLE_NAME, null);	
				
				while(rs.next()){
				      if(queryStr == null){
				    	  if(rs.getString("TYPE_NAME").equals("INTEGER") | rs.getString("TYPE_NAME").equals("TIMESTAMP") | rs.getString("TYPE_NAME").equals("DATE")){
				    		  queryStr = rs.getString("COLUMN_NAME")+" "+rs.getString("TYPE_NAME");
				    		  columnCount++;
				    	  } else {
				    		  if(rs.getString("TYPE_NAME").equals("NUMERIC")){
				    			  queryStr = rs.getString("COLUMN_NAME")+" "+rs.getString("TYPE_NAME")+"("+rs.getString("COLUMN_SIZE")+","+rs.getString("DECIMAL_DIGITS")+")";
				    			  columnCount++;
				    		  } else {
				    			  queryStr = rs.getString("COLUMN_NAME")+" "+rs.getString("TYPE_NAME")+"("+rs.getString("COLUMN_SIZE")+")";
				    			  columnCount++;
				    		  }				    		  
				    	  }
				    	  
				      } else {
				    	  if(rs.getString("TYPE_NAME").equals("INTEGER") | rs.getString("TYPE_NAME").equals("TIMESTAMP") | rs.getString("TYPE_NAME").equals("DATE")){
				    		  queryStr += ","+rs.getString("COLUMN_NAME")+" "+rs.getString("TYPE_NAME");
				    		  columnCount++;
				    	  } else {
				    		  if(rs.getString("TYPE_NAME").equals("NUMERIC")){
				    			  queryStr += ","+rs.getString("COLUMN_NAME")+" "+rs.getString("TYPE_NAME")+"("+rs.getString("COLUMN_SIZE")+","+rs.getString("DECIMAL_DIGITS")+")";
				    			  columnCount++;
				    		  } else {
				    			  queryStr += ","+rs.getString("COLUMN_NAME")+" "+rs.getString("TYPE_NAME")+"("+rs.getString("COLUMN_SIZE")+")";
				    			  columnCount++;
				    		  }				    		  
				    	  }
				    	  
				      }
				} 				
				isSuccess = createTable(TABLE_NAME, queryStr, columnCount);
				queryStr = null;
				columnCount = 0;
			}			
		} catch (SQLException e) {
			System.out.println("failed to Iterate DB Information !!");
		} finally {
			try {
				if(rs != null)rs.close();
			} catch (Exception e2) {
				Logging.error("Failed to close resuorces !!", e2);
			}
		}
		return isSuccess;
	}
	
	private boolean createTable(String tableName, String tableContent, int columnCount){
		boolean isCreated = false;
		Statement st = null, prodStmt = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try {
			st = hSqlConn.createStatement();
			//st.execute("drop table "+tableName+" IF EXISTS");
			int i = 0;//st.executeUpdate("create table "+tableName+"("+ tableContent +")");
			if(i>=0 && Data.equals("-WITHDATA")){
				Logging.info(tableName + " created successfully !!!!!!!!");
				prodStmt = prodServerConn.createStatement();
				String insertQuery = createInsertStatement(tableName, columnCount);
				rs = prodStmt.executeQuery("select * from "+tableName);
				pst = hSqlConn.prepareStatement(insertQuery);
				while(rs.next()){					
					for (int j = 1; j <= columnCount; j++) {
						pst.setObject(j, rs.getObject(j));						
					}
					pst.execute();
				}
			}			
		} catch (SQLException e) {
			Logging.error("Failed to create table = "+tableName, e);
		}		
		return isCreated;
	}
	
	private void dropTables(){
		Statement st = null;
		try {
			st = hSqlConn.createStatement();
			List<String> list = getTableNames();
			Iterator itr = list.iterator();
			while(itr.hasNext()){
				String table_name = (String) itr.next();
				st.execute("drop table "+table_name+" IF EXISTS");
				Logging.info("Table "+table_name+" dropped successfully !!");
			}
		} catch (SQLException e) {
			System.out.println("Failed to drop tables !!");
			e.printStackTrace();
		}		
	}
	
	private String createInsertStatement(String tblName, int count){
		String placeHolders = null;
		for(int i = 0;i<count;i++){
			if(placeHolders == null){
				placeHolders = "?";
			} else {
				placeHolders += ",?";
			}
		}
		
		return "insert into "+tblName+" values("+ placeHolders +")";
	}
	public static void main(String[] args) {
		moduleName = args[0];
		Data = args[1];
		System.out.println("moduleName == "+ moduleName);
		ReplicateData repData = new ReplicateData();
		boolean isSuccess = repData.iterateDBInfo();
		//repData.dropTables();		
		System.out.println("Completed successfully !!");		
	}

}
