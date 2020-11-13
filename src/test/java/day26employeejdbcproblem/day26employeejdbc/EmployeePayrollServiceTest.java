package day26employeejdbcproblem.day26employeejdbc;


import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.restassured.RestAssured;
import io.restassured.response.Response;


public class EmployeePayrollServiceTest 
{
	public EmployeePayrollService employeePayrollService= new EmployeePayrollService();
   
    public static final Logger log = LogManager.getLogger(EmployeePayrollService.class);
	@Test
    public void givenEmployeePayrollnDB_whenRetrieved_shouldMatchEmplyeeCount() {
    List<EmployeePayrollData> employeePayrollList = null;
	try {
		try {
			employeePayrollList = this.employeePayrollService.readEmployeePayrollData();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		Assert.assertEquals(3,employeePayrollList.size());
	} catch (EmployeePayrollServiceException e) {
		log.info(e.getMessage());
	}	
    }
	
	
    
}