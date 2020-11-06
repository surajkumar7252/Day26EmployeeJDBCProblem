package day26employeejdbcproblem.day26employeejdbc;


import java.sql.Connection;
import java.sql.DriverManager;
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
	public ResultSet resultSetOpted;

	public Connection connectingToDatabase() throws EmployeePayrollServiceException {

		String jdbcurl = "jdbc:mysql://127.0.0.1:3306/payroll_service?useSSL=false";
		String userName = "root";
		String password = "Heybro@1234";
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
		String query = "select * from employee_payroll";
		List<EmployeePayrollData> employeePayrollDataList = new ArrayList<EmployeePayrollData>();
		try {
			connection = this.connectingToDatabase();
			statementOpted = connection.createStatement();
			resultSetOpted = statementOpted.executeQuery(query);
			do {
				Integer id = resultSetOpted.getInt("id");
				String name = resultSetOpted.getString("name");
				String gender = resultSetOpted.getString("gender");
				Double salary = resultSetOpted.getDouble("salary");
				LocalDate start = resultSetOpted.getDate("start").toLocalDate();
				employeePayrollDataList.add(new EmployeePayrollData(id, name,  gender,salary, start));
			}while (resultSetOpted.next());

		} catch (SQLException e) {
			throw new EmployeePayrollServiceException("Reading Error.");
		} finally {
			if (connection != null)
				connection.close();
		}
		return employeePayrollDataList;

	}

	private static void listDrivers() {
		Enumeration<java.sql.Driver> driverList = DriverManager.getDrivers();
		while (driverList.hasMoreElements()) {
			Driver driverClass = (Driver) driverList.nextElement();
			log.info("  " + driverClass.getClass().getName());

		}

	}
	
    public static void main(String[] args) throws EmployeePayrollServiceException, SQLException {
		employeePayrollService.connectingToDatabase();
		employeePayrollService.readEmployeePayrollData();
		
	}
}