package day26employeejdbcproblem.day26employeejdbc;




import java.time.LocalDate;

public class EmployeePayrollData {
	public  Integer id;
	public String name;
	public Double salary;
	public String gender;
	public LocalDate start;
	public String phone_number;
	public String address;
	public String company;
	public EmployeePayrollData(Integer id, String name, String gender, Double salary,LocalDate start) {
		this.id = id;
		this.name = name;
		this.salary = salary;
		this.gender = gender;
		this.start=start;	
	}
	
    public EmployeePayrollData(Integer id, String name, String gender, Double salary) {
    	this.id = id;
		this.name = name;
		this.salary = salary;
		this.gender = gender;
	}

	public EmployeePayrollData(String company, Integer id, String name, String gender, Double salary,
			LocalDate startDate, String address, String phone_number) {
		this.id = id;
		this.company=company;
		this.name = name;
		this.salary = salary;
		this.gender = gender;
		this.start=startDate;
		this.address=address;
		this.phone_number=phone_number;
	}
	public String getPhone_number() {
		return phone_number;
	}

	public void setPhone_number(String phone_number) {
		this.phone_number = phone_number;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getCompany() {
		return company;
	}

	public void setCompany(String company) {
		this.company = company;
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
	
	
	public boolean equals(Object obj) {
		if(this.equals(obj)) return true;
		if(obj==null||getClass()!=obj.getClass())
			 return false;
		EmployeePayrollData epmData=(EmployeePayrollData) obj;
		return (id== epmData.id &&
				Double.compare(epmData.salary, salary)==0 &&
				name.equals(epmData.name));		
	
		}

}