package com.ebay.kvstore.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.IKVConstants;

public class ConfigurationLoader {
	private static Logger logger = LoggerFactory.getLogger(ConfigurationLoader.class);

	/**
	 * Load properties by the following sequence: 1. load
	 * kvstore.default.properties. 2. load kvstore.properties
	 * 
	 * @return
	 * @throws IOException
	 */
	public static IConfiguration load() throws IOException {
		IConfiguration defaultConf = load(IKVConstants.Default_Conf_Path);
		IConfiguration conf = null;
		try {
			conf = load(IKVConstants.Conf_Path);
		} catch (Exception e) {
			logger.warn("Fail to load property file:" + IKVConstants.Conf_Path, e);
		}
		if (conf != null) {
			defaultConf.merge(conf);
		}
		return defaultConf;
	}

	public static IConfiguration load(String path) throws IOException {
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		if (cl == null) {
			ConfigurationLoader.class.getClassLoader();
		}
		InputStream in = cl.getResourceAsStream(path);
		try {
			Properties p = new Properties();
			if (in != null) {
				p.load(in);
			}
			return new KVConfiguration(p);
		} finally {
			if (in != null) {
				in.close();
			}
		}
	}
}
