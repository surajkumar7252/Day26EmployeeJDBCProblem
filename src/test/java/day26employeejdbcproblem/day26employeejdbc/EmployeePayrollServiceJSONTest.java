package day26employeejdbcproblem.day26employeejdbc;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

public class EmployeePayrollServiceJSONTest {
	 public static final Logger log = LogManager.getLogger(EmployeePayrollService.class);
	 List<EmployeePayrollData> employeePayrollList;
	 private EmployeePayrollService employeePayrollService= new EmployeePayrollService(employeePayrollList);
	
	
	@Before
	public void setup() {
		RestAssured.baseURI="http://localhost";
		RestAssured.port=3000;
	}
	
	@Test
	public void employeeDataWhenGivenInJsonServer_whenRetrieved_shouldMatchTheCount() {
		EmployeePayrollData[] arrayOfEmps=getEmployeeList();
		EmployeePayrollService employePayrollService;
		employeePayrollService=new EmployeePayrollService(Arrays.asList(arrayOfEmps));
		long entries=employeePayrollService.employeePayrollDBList.size();
		Assert.assertEquals(2, entries);
	}

	public EmployeePayrollData[] getEmployeeList() {

        Response response =RestAssured.get("/employee_payroll");
        log.info("Employee Payroll Entries in JsonServer: \n"+response.asString());
        EmployeePayrollData[] arrayOfEmps=new Gson().fromJson(response.asString(),EmployeePayrollData[].class);
        return arrayOfEmps;
	}
	
    public Response addNewEmployeeToJsonServer(EmployeePayrollData employeePayrollData) {
		String empJson=new Gson().toJson(employeePayrollData);
		RequestSpecification request=RestAssured.given();
		request.header("Content-Type","application/json");
		request.body(empJson);
		return request.post("/employee_payroll");
	}
  

    @Test
    public void employeeDataWhenGivenInJsonServer_whenAdded_shouldMatchResponseAndCount() throws EmployeePayrollServiceException, SQLException
    {
    	EmployeePayrollService employeePayrollService;;
    	EmployeePayrollData[] arrayOfEmployees = getEmployeeList();
    	employeePayrollService=new EmployeePayrollService(Arrays.asList(arrayOfEmployees));
    	LocalDate startDate=LocalDate.of(2017, 1, 13);
    	EmployeePayrollData employeePayrollData=new EmployeePayrollData("ABC_COMPANY","PRAKASH", "M", 950000.00,startDate, "Dhanbad", "885522669933");
    	Response response=addNewEmployeeToJsonServer(employeePayrollData);
    	int statusCode=response.getStatusCode();
    	Assert.assertEquals(201, statusCode);
    	employeePayrollData=new Gson().fromJson(response.asString(),EmployeePayrollData.class);
    	employeePayrollService.addNewEmployeeToJsonServerUsingRestAPI(employeePayrollData);
    	long entries=employeePayrollService.employeePayrollDBList.size();
    	Assert.assertEquals(5, entries);
    }
    
    @Test
    public void multipleEmployeeDataWhenGivenInJsonServer_whenAdded_shouldMatchResponseAndCount() throws EmployeePayrollServiceException, SQLException
    {
    	EmployeePayrollService employeePayrollService;;
    	EmployeePayrollData[] arrayOfEmployees = getEmployeeList();
    	employeePayrollService=new EmployeePayrollService(Arrays.asList(arrayOfEmployees));
    	LocalDate startDate=LocalDate.of(2017, 1, 13);
    	EmployeePayrollData[] arrayOfEmployeePayrollData= {new EmployeePayrollData("ABC_COMPANY","PRAKASH", "M", 950000.00,startDate, "Dhanbad", "885522669933"),
    			                                    new EmployeePayrollData("XYZ_COMPANY","Abhay", "M", 1050000.00,startDate, "Gaya", "885522600225"),
    			                                    new EmployeePayrollData("PQR_COMPANY","Ravi", "M", 250000.00,startDate, "Dhanbad", "885522633225"),
    			                                     };
    	for(EmployeePayrollData employeePayrollData:arrayOfEmployeePayrollData) {
        Response response=addNewEmployeeToJsonServer(employeePayrollData);
    	int statusCode=response.getStatusCode();
    	Assert.assertEquals(201, statusCode);
    	employeePayrollData=new Gson().fromJson(response.asString(),EmployeePayrollData.class);
    	employeePayrollService.addNewEmployeeToJsonServerUsingRestAPI(employeePayrollData);
    	}
    	long entries=employeePayrollService.employeePayrollDBList.size();
    	Assert.assertEquals(8, entries);
    }
    @Test
    public void newSalaryWhenGivenForEmployee_whenUpdated_shouldMatch200Response() throws EmployeePayrollServiceException, SQLException
    {
    	EmployeePayrollService employeePayrollService;;
    	EmployeePayrollData[] arrayOfEmployees = getEmployeeList();
    	employeePayrollService=new EmployeePayrollService(Arrays.asList(arrayOfEmployees));
    	LocalDate startDate=LocalDate.of(2017, 1, 13);
    	employeePayrollService.updateEmployeePayrollDataUsingStatement("PRAKASH",54321000.00);
    	EmployeePayrollData employeePayrollData= (EmployeePayrollData) employeePayrollService.readEmployeePayrollDataFromDataBase("PRAKASH");
         String empJson=new Gson().toJson(employeePayrollData);
		RequestSpecification request=RestAssured.given();
		request.header("Content-Type","application/json");
		request.body(empJson);
	    Response response=request.put("/employee_payroll/"+employeePayrollData.id);
	     int statusCode=response.getStatusCode();
    	Assert.assertEquals(200, statusCode);
    	
    }
    
    @Test
    public void employeeToDeleteWhenSaid_shouldMatch200ResponseAndCount() throws EmployeePayrollServiceException, SQLException
    {
    	EmployeePayrollService employeePayrollService;;
    	EmployeePayrollData[] arrayOfEmployees = getEmployeeList();
    	employeePayrollService=new EmployeePayrollService(Arrays.asList(arrayOfEmployees));
    	EmployeePayrollData employeePayrollData= (EmployeePayrollData) employeePayrollService.readEmployeePayrollDataFromDataBase("PRAKASH");
    	RequestSpecification request=RestAssured.given();
		request.header("Content-Type","application/json");
		 Response response=request.delete("/employee_payroll/"+employeePayrollData.id);
		int statusCode=response.getStatusCode();
    	Assert.assertEquals(200, statusCode);
    	employeePayrollService.deleteEmployeeDetails(employeePayrollData.name); 
    	 
    	long entries=employeePayrollService.employeePayrollDBList.size();
    	Assert.assertEquals(7, entries);
    }
}
