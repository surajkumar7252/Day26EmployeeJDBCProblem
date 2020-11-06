package day26employeejdbcproblem.day26employeejdbc;



import java.time.LocalDate;

public class EmployeePayrollData {
	public  Integer id;
	public String name;
	public Double salary;
	public String gender;
	public LocalDate start;
	public EmployeePayrollData(Integer id, String name, String gender, Double salary,LocalDate start) {
		this.id = id;
		this.name = name;
		this.salary = salary;
		this.gender = gender;
		this.start=start;	
	}
	
    public Integer getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Double getSalary() {
		return salary;
	}

	public void setSalary(double salary) {
		this.salary = salary;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public LocalDate getStart() {
		return start;
	}

	public void setStart(LocalDate start) {
		this.start = start;
	}
	

	public String toString() {
		return "Employee ID = " + id + ","+"Emp_Name = " + name + "," + " Emp_Salary = "+", " + salary + ","+" Gender = " + gender+","
				+ " Start = " + start ;
	}
	
	
	
}

