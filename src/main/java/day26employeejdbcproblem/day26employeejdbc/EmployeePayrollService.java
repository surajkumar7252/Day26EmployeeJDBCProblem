package day26employeejdbcproblem.day26employeejdbc;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Enumeration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mysql.cj.jdbc.Driver;

public class EmployeePayrollService {
	

	
		public static final Logger log = LogManager.getLogger(EmployeePayrollService.class);
	    public static void main( String[] args )
	    {   
	        String jdbcurl="jdbc:mysql://127.0.0.1:3306/payroll_service?useSSL=false";
	        String userName="root";
	        String password="Heybro@1234";
	        Connection connection;
	        try {
	        	Class.forName("com.mysql.cj.jdbc.Driver");
	        	log.info("Drivers Loaded");
	        }catch(ClassNotFoundException e) {
	        	throw new IllegalStateException("Can't find the driver in the class path.");
	        }
	        listDrivers();
	        		try {
	        			log.info("Connecting to database: "+jdbcurl);
	        			connection = DriverManager.getConnection(jdbcurl,userName,password);
	        			log.info("Connection is successful ");
	        			
	        		}catch(Exception e) {
	        			e.printStackTrace();
	        			
	        		}
	    }

		private static void listDrivers() {
			Enumeration<java.sql.Driver> driverList=DriverManager.getDrivers();
			while(driverList.hasMoreElements()) {
				Driver driverClass=(Driver) driverList.nextElement();
				log.info("  "+ driverClass.getClass().getName());
				
			}
			
		}
	}


