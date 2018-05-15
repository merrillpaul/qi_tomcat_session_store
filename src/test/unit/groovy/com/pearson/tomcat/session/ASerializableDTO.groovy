package com.pearson.tomcat.session

class ASerializableDTO implements Serializable {
	String name
	Integer salary
	ASerializableDTO parent


	@Override
	public String toString() {
		return "ASerializableDTO{" +
				"name='" + name + '\'' +
				", salary=" + salary +
				", parent=" + parent +
				'}';
	}
}
