package day26employeejdbcproblem.day26employeejdbc;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.sql.ResultSet;
import java.sql.Statement;
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
	private List<EmployeePayrollData> employeePayrollDBList;
	static LocalDate startDate;
	
	
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
		

	}

	public Connection connectingToDatabase() throws EmployeePayrollServiceException {

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
			connection = DriverManager.getConnection(jdbcurl, userName, password);
			log.info("Connection is successful ");
			return connection;

		} catch (SQLException e) {
			throw new EmployeePayrollServiceException("Connection failed");

		}
	}

	public List<EmployeePayrollData> readEmployeePayrollData() throws EmployeePayrollServiceException, SQLException {
		List<EmployeePayrollData> employeePayrollDataList = new ArrayList<EmployeePayrollData>();
		String query = "select * from employee_payroll";

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
		String query = String.format("update emplyee_Payroll set salary=%f where name='%s'", salary, name);
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
		String query = String.format("select * from employee_payroll where name='%s'", name);
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
			String query="update employee_payroll set name=? where salary=? ";
			
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
		List<EmployeePayrollData> employeePayrollData=employeePayrollService.employeePayrollService.readEmployeePayrollDataFromDataBase(name);
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
				String query = "select * from employee_payroll where name=?";
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
		String query = String.format("select * from employee_payroll where start between cast('%s' as date) and cast('%s' as date);",startDate, endDate);
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
		case AVG:query=String.format("select %sGENDER,%d AVG(SALARY) from employee_payroll group by gender");
		         break;
		case SUM:query=String.format("select %sGENDER,%dSUM(SALARY) from employee_payroll group by gender");
                 break;   
		case COUNT:query=String.format("select %sGENDER,%dCOUNT(SALARY) from employee_payroll group by gender");
                 break;
		case MIN:query=String.format("select %sGENDER,%dSUM(SALARY) from employee_payroll group by gender");
                break;
		case MAX:query=String.format("select %sGENDER,%dSUM(SALARY) from employee_payroll group by gender");
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


}