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

	public static void main(String[] args) throws EmployeePayrollServiceException, SQLException {
		employeePayrollService.connectingToDatabase();
		employeePayrollService.readEmployeePayrollData();
		employeePayrollService.updateEmployeePayrollDataUsingStatement("SURAJ", 950000.00);
		employeePayrollService.readEmployeePayrollDataFromDataBase("SURAJ");

	}

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
		List<EmployeePayrollData> employeePayrollDataList = new ArrayList<EmployeePayrollData>();
		String query = "select * from employee_payroll";

		try {
			connection = this.connectingToDatabase();
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
		String query = String.format("update emplyee_Payroll set salary=950000.00f where name='SURAJ'", salary, name);
		try {
			connection = this.connectingToDatabase();
			statementOpted = connection.createStatement();
			Integer updateApplied = statementOpted.executeUpdate(query);
			log.info(" Updations: " + updateApplied);
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
			connection = this.connectingToDatabase();
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

	private static void listDrivers() {
		Enumeration<java.sql.Driver> driverList = DriverManager.getDrivers();
		while (driverList.hasMoreElements()) {
			Driver driverClass = (Driver) driverList.nextElement();
			log.info("  " + driverClass.getClass().getName());

		}

	}

}