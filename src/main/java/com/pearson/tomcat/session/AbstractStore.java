package com.pearson.tomcat.session;

import org.apache.catalina.Container;
import org.apache.catalina.session.StoreBase;

public abstract class AbstractStore extends StoreBase {
	/**
	 * Context name associated with this Store
	 */
	protected String name = null;
	/**
	 * @return the name for this instance (built from container name)
	 */
	public String getName() {
		if (name == null) {
			Container container = manager.getContext();
			String contextName = container.getName();
			if (!contextName.startsWith("/")) {
				contextName = "/" + contextName;
			}
			String hostName = "";
			String engineName = "";

			if (container.getParent() != null) {
				Container host = container.getParent();
				hostName = host.getName();
				if (host.getParent() != null) {
					engineName = host.getParent().getName();
				}
			}
			name = "/" + engineName + "/" + hostName + contextName;
		}
		return name;
	}
}
