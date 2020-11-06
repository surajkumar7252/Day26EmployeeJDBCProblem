package day26employeejdbcproblem.day26employeejdbc;


import java.sql.SQLException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;


public class EmployeePayrollServiceTest 
{
	private EmployeePayrollService employeePayrollService= new EmployeePayrollService();
   
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