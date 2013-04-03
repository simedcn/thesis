package com.ebay.kvstore.server.master.balancer;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.conf.IConfigurationKey;
import com.ebay.kvstore.conf.InvalidConfException;

public class LoadBalancerFactory {

	public static ILoadBalancer createLoadBalancer(IConfiguration conf) {
		ILoadBalancer balancer = null;
		String policy = conf.get(IConfigurationKey.Master_LoadBalance_Policy);
		switch (policy) {
		case "simple":
			balancer = new SimpleLoadBalancer(conf);
			break;
		case "advanced":
			balancer = new AdvancedLoadBalancer(conf);
			break;
		default:
			throw new InvalidConfException(IConfigurationKey.Master_LoadBalance_Policy,
					"simple|advanced", policy);
		}
		return balancer;
	}
}
