package com.bd.common;

/**
 * @author yuan.li
 *
 */
public class CustModel {
	private String firstname;
	private String secondname;
	private String sex;
	private Integer age;

	public String getFirstname() {
		return firstname;
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}

	public String getSecondname() {
		return secondname;
	}

	public void setSecondname(String secondname) {
		this.secondname = secondname;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	@Override
	public String toString() {
		return "CustModel [firstname=" + firstname + ", secondname=" + secondname + ", sex=" + sex + ", age=" + age + "]";
	}
}
