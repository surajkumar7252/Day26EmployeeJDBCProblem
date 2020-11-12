package day26employeejdbcproblem.day26employeejdbc;




import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.mysql.cj.jdbc.Driver;

public class EmployeePayrollService {
	public static final Logger log = LogManager.getLogger(EmployeePayrollService.class);
	public static EmployeePayrollService employeePayrollService = new EmployeePayrollService();
	public Connection connection;
	public Statement statementOpted;
	public static ResultSet resultSetOpted;
	public PreparedStatement preparedSqlStatement;
	public  List<EmployeePayrollData> employeePayrollDBList;
	public static List<EmployeePayrollData>threadedEmployeeList;
	static LocalDate startDate;
	public int connectionCounter=1;
	
	
	static Double femaleResult = 0.0;
	static Double maleResult = 0.0;
	
	public enum TypeOfCalculation {
		AVG, SUM, MIN, MAX, COUNT
	}
	public TypeOfCalculation calcType;

	public static void main(String[] args) throws EmployeePayrollServiceException, SQLException {
		employeePayrollService.connectingToDatabase();
	    employeePayrollService.readEmployeePayrollData();
		employeePayrollService.updateEmployeePayrollDataUsingStatement("SURAJ", 950000.00);
		employeePayrollService.readEmployeePayrollDataFromDataBase("SURAJ");
		
		employeePayrollService.updateEmployeePayrollDataUsingPrepredStatement("SURAJ", 950000.00);
		employeePayrollService.checkSyncWithDB("SURAJ");
		employeePayrollService.readEmployeePayrollDataFromResultset(resultSetOpted);
		startDate=LocalDate.of(2017, 1, 13);
		employeePayrollService.getEmployeePayrollDataByDateOfStarting(startDate, LocalDate.now());
		employeePayrollService.makeComputations(TypeOfCalculation.AVG);
		employeePayrollService.addEmployeeToPayrollDB("Capgemini","SURAJ", "M", 950000.00,startDate, "Dhanbad", "885522669933");
		employeePayrollService.addMultipleEmployeeToPayrollDBWithoutUsingMultiThreading(threadedEmployeeList);
		employeePayrollService.addMultipleEmployeeToPayrollDBUsingMultiThreading(threadedEmployeeList);

	}

	public synchronized Connection connectingToDatabase() throws EmployeePayrollServiceException {
		connectionCounter++;
		String jdbcurl = "jdbc:mysql://127.0.0.1:3306/payroll_service?useSSL=false";
		String userName = "root";
		String password = "HeyBro@1234";
		Connection connection;
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			log.info("Drivers Loaded");

		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("Can't find the driver in the class path.");
		}
		listDrivers();
		try {
			log.info("Connecting to database: " + jdbcurl);
			log.info("Processing Thread : "+Thread.currentThread().getName() + " Connecting to database with Id: "+ connectionCounter);
         	connection = DriverManager.getConnection(jdbcurl, userName, password);
			log.info("Connection is successful ");
			return connection;

		} catch (SQLException e) {
			throw new EmployeePayrollServiceException("Connection failed");

		}
	}

	public List<EmployeePayrollData> readEmployeePayrollData() throws EmployeePayrollServiceException, SQLException {
		List<EmployeePayrollData> employeePayrollDataList = new ArrayList<EmployeePayrollData>();
		String query = "select * from (employee inner join payroll on employee.PERSONAL_ID=payroll.PERSONAL_ID)";

		try {
			connection = employeePayrollService.connectingToDatabase();
			statementOpted = connection.createStatement();
			resultSetOpted = statementOpted.executeQuery(query);
			do {
				Integer id = resultSetOpted.getInt("ID");
				String name = resultSetOpted.getString("NAME");
				String gender = resultSetOpted.getString("GENDER");
				Double salary = resultSetOpted.getDouble("SALARY");
				LocalDate start = resultSetOpted.getDate("START").toLocalDate();
				employeePayrollDataList.add(new EmployeePayrollData(id, name, gender, salary, start));
			} while (resultSetOpted.next());

		} catch (SQLException e) {
			throw new EmployeePayrollServiceException("Reading Error.");
		} finally {
			if (connection != null)
				connection.close();
		}
		return employeePayrollDataList;

	}

	private void updateEmployeePayrollDataUsingStatement(String name, Double salary)
			throws EmployeePayrollServiceException, SQLException {
		String query = String.format("update emplyee_Payroll set NET_PAY=%f where EMP_NAME='%s'", salary, name);
		try {
			connection = employeePayrollService.connectingToDatabase();
			statementOpted = connection.createStatement();
			statementOpted.executeUpdate(query);
			log.info("Updation Complete");
		} catch (SQLException e) {
			throw new EmployeePayrollServiceException("Updation Failed");

		} finally {
			if (connection != null)
				connection.close();
		}
	}

	
	public List<EmployeePayrollData> readEmployeePayrollDataFromDataBase(String name)
			throws EmployeePayrollServiceException, SQLException {
		
		List<EmployeePayrollData> employeePayrollList = new ArrayList<EmployeePayrollData>();
		String query = String.format("select * from (employee inner join payroll on employee.PERSONAL_ID=payroll.PERSONAL_ID) where EMP_NAME='%s'", name);
		try {
			connection = employeePayrollService.connectingToDatabase();
			statementOpted = connection.createStatement();
			resultSetOpted = statementOpted.executeQuery(query);
			do {
				Integer idOfEmployee = resultSetOpted.getInt("ID");
				String nameOfEmployee = resultSetOpted.getString("NAME");
				String genderOfEmployee = resultSetOpted.getString("GENDER");
				Double salaryOfEmployee = resultSetOpted.getDouble("SALARY");
				LocalDate startDateOfEmployee = resultSetOpted.getDate("START").toLocalDate();
				employeePayrollList.add(new EmployeePayrollData(idOfEmployee, nameOfEmployee, genderOfEmployee,
						salaryOfEmployee, startDateOfEmployee));
			} while (resultSetOpted.next());
			return employeePayrollList;
		} catch (SQLException e) {
			throw new EmployeePayrollServiceException("Reading Error");
		} finally {
			if (connection != null)
				connection.close();
		}
	}

	public void updateEmployeePayrollDataUsingPrepredStatement(String name, Double salary) throws EmployeePayrollServiceException, SQLException {
		
		try {
			connection=employeePayrollService.connectingToDatabase();
			String query="update payroll set NET_PAY=? where PERSONAL_ID =? ";
			
			employeePayrollService.preparedSqlStatement=connection.prepareStatement(query);
		}catch (SQLException e) {
			throw new EmployeePayrollServiceException("Preparation Failed");
		}
		
		try {
			
			preparedSqlStatement.setString(1, name);
			preparedSqlStatement.setDouble(2, salary);
			preparedSqlStatement.executeUpdate();
			log.info("Updation Complete");
		}catch(SQLException e) {
			throw new EmployeePayrollServiceException("Preparation Failed");
		}
	 finally {
		if (connection != null)
			connection.close();
	}
		
}
	
	public boolean checkSyncWithDB(String name) throws EmployeePayrollServiceException, SQLException {
		List<EmployeePayrollData> employeePayrollData=employeePayrollService.readEmployeePayrollDataFromDataBase(name);
		return employeePayrollData.get(0).equals(employeePayrollDBList.stream()
				.filter(employeePayrollObject->employeePayrollObject.getName().equals(name))
				.findFirst().orElse(null));
	}
	private static void listDrivers() {
		Enumeration<java.sql.Driver> driverList = DriverManager.getDrivers();
		while (driverList.hasMoreElements()) {
			Driver driverClass = (Driver) driverList.nextElement();
			log.info("  " + driverClass.getClass().getName());

		}

	}
	private List<EmployeePayrollData> readEmployeePayrollDataFromResultset(ResultSet resultSet)
			throws EmployeePayrollServiceException, SQLException {
		employeePayrollDBList = new ArrayList<EmployeePayrollData>();
		try {
			try {
				connection=employeePayrollService.connectingToDatabase();
				String query = "select * from (employee inner join payroll on employee.PERSONAL_ID=payroll.PERSONAL_ID) where EMP_NO=?";
				employeePayrollService.preparedSqlStatement = connection.prepareStatement(query);	
				} catch (SQLException e) {
				throw new EmployeePayrollServiceException("Preparation Failed.");
			}
			 
			
			
			do{
				Integer id = resultSet.getInt("ID");
				String objectname = resultSet.getString("NAME");
				String gender = resultSet.getString("GENDER");
				Double salary = resultSet.getDouble("SALARY");
				LocalDate start = resultSet.getDate("START").toLocalDate();
				employeePayrollDBList.add(new EmployeePayrollData(id, objectname, gender, salary, start));
			}while (resultSet.next()) ;
			
			return employeePayrollDBList;
		} catch (SQLException e) {
			throw new EmployeePayrollServiceException("Reusing Result Set failed.");
		}
		finally {
			if (connection != null)
				connection.close();
		}
		
	}
	
	public List<EmployeePayrollData> getEmployeePayrollDataByDateOfStarting(LocalDate startDate, LocalDate endDate)
			throws EmployeePayrollServiceException, SQLException {
		String query = String.format("select * from (employee inner join payroll on employee.PERSONAL_ID=payroll.PERSONAL_ID) where start between cast('%s' as date) and cast('%s' as date);",startDate, endDate);
		try {
			connection=employeePayrollService.connectingToDatabase();
			statementOpted = connection.createStatement();
			 resultSetOpted = statementOpted.executeQuery(query);
			return employeePayrollService.readEmployeePayrollDataFromResultset(resultSetOpted);
		} catch (SQLException e) {
			throw new EmployeePayrollServiceException("Connection Failed.");
		}
		finally {
			if (connection != null)
				connection.close();
		}
	}
	
	public void makeComputations(TypeOfCalculation calculationType) throws EmployeePayrollServiceException, SQLException {
		Double maleCalcResult=0.0;
		Double femaleCalcResult=0.0;
		String query=null;
		switch(calculationType) {
		case AVG:query=String.format("select %sGENDER,%d AVG(SALARY) from employee inner join payroll on employee.PERSONAL_ID=payroll.PERSONAL_ID  group by GENDER");
		         break;
		case SUM:query=String.format("select %sGENDER,%dSUM(SALARY) from employee inner join payroll on employee.PERSONAL_ID=payroll.PERSONAL_ID  group by GENDER");
                 break;   
		case COUNT:query=String.format("select %sGENDER,%dCOUNT(SALARY) from employee inner join payroll on employee.PERSONAL_ID=payroll.PERSONAL_ID  group by GENDER");
                 break;
		case MIN:query=String.format("select %sGENDER,%dSUM(SALARY) from employee inner join payroll on employee.PERSONAL_ID=payroll.PERSONAL_ID  group by GENDER");
                break;
		case MAX:query=String.format("select %sGENDER,%dSUM(SALARY) from employee inner join payroll on employee.PERSONAL_ID=payroll.PERSONAL_ID  group by GENDER");
                   break;
		}
		try {
			connection=employeePayrollService.connectingToDatabase();
			statementOpted = connection.createStatement();
			 resultSetOpted = statementOpted.executeQuery(query);
			
			while(resultSetOpted.next()) {
				if(resultSetOpted.getString("GENDER").equals("M")) maleCalcResult=resultSetOpted.getDouble("SALARY");
				else femaleCalcResult=resultSetOpted.getDouble("SALARY");
			}
			log.info("Female Total calculation"+femaleCalcResult);
			log.info("Male Total calculation"+maleCalcResult);
			
		} catch (SQLException e) {
			throw new EmployeePayrollServiceException("Unable to use resultset");
		}
		finally {
			if (connection != null)
				connection.close();
		}
	}


     public void addEmployeeToPayrollDB(String company,String name, String gender, Double salary, LocalDate startDate, String address, String phone_number) throws EmployeePayrollServiceException, SQLException {
    	 EmployeePayrollData employeePayrollData;
    	 String query1 = String.format("insert into employee_payroll (NAME,GENDER,SALARY,STARTDATE) values ('%s','%s',%f,'%s')",name, gender, salary, startDate);
		try {
			connection.setAutoCommit(false);
			connection=employeePayrollService.connectingToDatabase();
			String query01=String.format("select ORGANISATION_ID  from organisation where ORGANISATION_NAME ='%s'", company);
			Statement statement01=connection.createStatement();
			ResultSet resultSet01=statement01.executeQuery(query01);	
			Integer org_Id = resultSet01.getInt("ORGANISATION_ID");
			
			String query02=String.format("insert into organisation(ORGANISATION_NAME)  values ('%s')", company);
			Statement statement02=connection.createStatement();
			statement02.executeQuery(query02);	
			
			String query03=String.format("insert into employee (EMP_NAME ,GENDER,START ,ORGANISATION_ID , ADDRESS , PHONE_NUMBER ) values ('%s','%s','%s',%s,'%s','%s')",name, gender, startDate, org_Id, address, phone_number);
			Statement statement03=connection.createStatement();
			ResultSet resultSet03=statement03.executeQuery(query03);	
			
			Integer objectId = resultSet03.getInt("ID");
			Double BASIC_PAY = salary;
			Double DEDUCTIONS = 0.2 * BASIC_PAY;
			Double TAXABLE_PAY = BASIC_PAY - DEDUCTIONS;
			Double INCOME_TAX = 0.1 * TAXABLE_PAY;
			Double NET_PAY = BASIC_PAY - INCOME_TAX;
			
			String query2 = String.format("insert into payroll (ID,BASIC_PAY,DEDUCTIONS,TAXABLE_PAY,INCOME_TAX,NET_PAY) values (%s,%f,%f,%f,%f,%f)", objectId, BASIC_PAY,
					DEDUCTIONS, TAXABLE_PAY, INCOME_TAX, NET_PAY);
			Statement statement = connection.createStatement();
			statement.executeQuery(query2);	
			employeePayrollData = new EmployeePayrollData(company,objectId, name, gender, salary,startDate,address,phone_number);
			connection.commit();
			
				} catch (SQLException e) {
					connection.rollback();
					throw new EmployeePayrollServiceException("Adding Details Failed");
		}
		finally {
			if (connection != null)
				connection.close();
		}
	}

     public void getDataFromDBAfterImplementingER(String name, Double salary) throws EmployeePayrollServiceException, SQLException {
 		
 		try {
 			connection=employeePayrollService.connectingToDatabase();
 			String query=" select GENDER,count(NET_PAY) from employee inner join payroll on employee.PERSONAL_ID=payroll.PERSONAL_ID  group by GENDER ";
 			statementOpted=connection.createStatement();
 			ResultSet resultSet=statementOpted.executeQuery(query);
 		    log.info(resultSet.getString("GENDER"));
 		    log.info(resultSet.getDouble("count(NET_PAY)"));
 			
 		}catch(SQLException e) {
 			throw new EmployeePayrollServiceException("Reading Data Failed");
 		}
 	 finally {
 		if (connection != null)
 			connection.close();
 	}
 		
 }
     
     public void deleteEmployeeDetails(String name) throws EmployeePayrollServiceException, SQLException {
 		List<EmployeePayrollData> employeeToDeleteDetails = employeePayrollService.readEmployeePayrollDataFromDataBase(name);
 		String query = String.format("update employee set is_active=0 where EMP_NAME='%s' and is_active",name);
 		try {
 			connection=employeePayrollService.connectingToDatabase();
 			statementOpted=connection.createStatement();
 			statementOpted.executeQuery(query);
 		    employeeToDeleteDetails.forEach(employeePayrollData  -> employeePayrollService.employeePayrollDBList.remove(employeePayrollData ));
 	}catch(SQLException e) {
			throw new EmployeePayrollServiceException("Deletion Failed");
		}
	 finally {
		if (connection != null)
			connection.close();
	}
     
     
     } 
     
     public void addMultipleEmployeeToPayrollDBWithoutUsingMultiThreading(List<EmployeePayrollData> employeesList) throws EmployeePayrollServiceException, SQLException {
    	 Instant start=Instant.now();
    	 
    	try {
    		for( EmployeePayrollData employeePayrollData:employeesList) {
       		 String name=employeePayrollData.getName();
       		 String company=employeePayrollData.getCompany();
       		 String gender=employeePayrollData.getGender();
       		 LocalDate startDate=employeePayrollData.getStart();
       		 String phone_number=employeePayrollData.getPhone_number();
       		 String address=employeePayrollData.getAddress();
       		 Double salary=employeePayrollData.getSalary();
			
			
			connection.setAutoCommit(false);
			connection=employeePayrollService.connectingToDatabase();
			String query01=String.format("select ORGANISATION_ID  from organisation where ORGANISATION_NAME ='%s'", company);
			Statement statement01=connection.createStatement();
			ResultSet resultSet01=statement01.executeQuery(query01);	
			Integer org_Id = resultSet01.getInt("ORGANISATION_ID");
			
			String query02=String.format("insert into organisation(ORGANISATION_NAME)  values ('%s')", company);
			Statement statement02=connection.createStatement();
			statement02.executeQuery(query02);	
			
			String query03=String.format("insert into employee (EMP_NAME ,GENDER,START ,ORGANISATION_ID , ADDRESS , PHONE_NUMBER ) values ('%s','%s','%s',%s,'%s','%s')",name, gender, startDate, org_Id, address, phone_number);
			Statement statement03=connection.createStatement();
			ResultSet resultSet03=statement03.executeQuery(query03);	
			
			Integer objectId = resultSet03.getInt("ID");
			Double BASIC_PAY = salary;
			Double DEDUCTIONS = 0.2 * BASIC_PAY;
			Double TAXABLE_PAY = BASIC_PAY - DEDUCTIONS;
			Double INCOME_TAX = 0.1 * TAXABLE_PAY;
			Double NET_PAY = BASIC_PAY - INCOME_TAX;
			
			String query2 = String.format("insert into payroll (ID,BASIC_PAY,DEDUCTIONS,TAXABLE_PAY,INCOME_TAX,NET_PAY) values (%s,%f,%f,%f,%f,%f)", objectId, BASIC_PAY,
					DEDUCTIONS, TAXABLE_PAY, INCOME_TAX, NET_PAY);
			Statement statement = connection.createStatement();
			statement.executeQuery(query2);	
			employeePayrollData = new EmployeePayrollData(company,objectId, name, gender, salary,startDate,address,phone_number);
			connection.commit();
    	}
    	 
			Instant end=Instant.now();
			log.info("Duration Without Thread : "+Duration.between(start,end));
				} catch (SQLException e) {
					connection.rollback();
					throw new EmployeePayrollServiceException("Adding Details Failed");
		}
		finally {
			if (connection != null)
				connection.close();
		}
	}
    
     public void addMultipleEmployeeToPayrollDBUsingMultiThreading(List<EmployeePayrollData> employeesList) throws EmployeePayrollServiceException, SQLException {
      Map<Integer,Boolean> employeeAddition=new HashMap<Integer,Boolean>();
    	 employeesList.forEach(employeePayrollData ->{
    		 Runnable task=()->{
    			 employeeAddition.put(employeePayrollData.hashCode(),false); 
                 System.out.println("Employee Being Added: "+Thread.currentThread().getName()); 
                 try {
					this.addEmployeeToPayrollDB(employeePayrollData.company, employeePayrollData.name,employeePayrollData.gender, employeePayrollData.salary, employeePayrollData.start, employeePayrollData.address, employeePayrollData.phone_number);
				} catch (EmployeePayrollServiceException | SQLException e) {
					
					e.printStackTrace();
				}
                 employeeAddition.put(employeePayrollData.hashCode(),true); 
    		     log.info("Employee Added : "+Thread.currentThread().getName());
    		 };
    		 Thread thread=new  Thread(task,employeePayrollData.name);
    	     thread.start();
    	 });
    	 while(employeeAddition.containsValue(false)) {
    		 try {
    			 Thread.sleep(1000);
    		 }catch(InterruptedException e) {
    			 e.printStackTrace();
    		 }
    	 }
       }
    
     public void updatingDataUsingThreading(List<EmployeePayrollData> employeesList) throws EmployeePayrollServiceException, SQLException {
         Map<Integer,Boolean> employeeAddition=new HashMap<Integer,Boolean>();
       	 employeesList.forEach(employeePayrollData ->{
       		 Runnable task=()->{
       			 employeeAddition.put(employeePayrollData.hashCode(),false); 
                    System.out.println("Employee Being Added: "+Thread.currentThread().getName()); 
                    try {
   					this.updateEmployeePayrollDataUsingPrepredStatement(employeePayrollData.name,employeePayrollData.salary);
   				} catch (EmployeePayrollServiceException | SQLException e) {
   					
   					e.printStackTrace();
   				}
                    employeeAddition.put(employeePayrollData.hashCode(),true); 
       		     log.info("Employee Updated : "+Thread.currentThread().getName());
       		 };
       		 Thread thread=new  Thread(task,employeePayrollData.name);
       	     thread.start();
       	 });
       	 while(employeeAddition.containsValue(false)) {
       		 try {
       			 Thread.sleep(1000);
       		 }catch(InterruptedException e) {
       			 e.printStackTrace();
       		 }
       	 }
          }
}