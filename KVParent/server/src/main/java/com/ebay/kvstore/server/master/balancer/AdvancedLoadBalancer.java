package com.ebay.kvstore.server.master.balancer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.structure.Address;
import com.ebay.kvstore.structure.DataServerStruct;
import com.ebay.kvstore.structure.Region;

public class AdvancedLoadBalancer extends BaseLoadBalancer {

	public AdvancedLoadBalancer(IConfiguration conf) {
		super(conf);
	}

	@Override
	public Map<Region, Address> assignRegion(Collection<Region> regions,
			Collection<DataServerStruct> dataServers) {
		boolean contains = true;
		for (Region region : regions) {
			if (!loadTargets.containsKey(region)) {
				contains = false;
				break;
			}
		}
		if (contains) {
			return Collections.unmodifiableMap(loadTargets);
		}
		RegionMetric regionMetric = calcRegionMetric(regions, dataServers);
		RegionWrapper[] regionWrappers = calcRegionWrappers(regions, dataServers, regionMetric);
		DataServerWrapper[] dataServerWrappers = calcDataServerWrappers(dataServers, regionMetric);
		int index = 0;
		double maxCapacity = dataServerWrappers[dataServerWrappers.length - 1].capacity;
		for (int i = 0; i < dataServerWrappers.length - 1; i++) {
			DataServerWrapper wrapper = dataServerWrappers[i];
			while (wrapper.capacity < maxCapacity && index < regionWrappers.length) {
				wrapper.capacity += (regionWrappers[index].factor / wrapper.factor);
				if (!loadTargets.containsKey(regionWrappers[index].region)) {
					loadTargets.put(regionWrappers[index].region, wrapper.ds.getAddr());
				}
				index++;
			}
			if (index == regionWrappers.length) {
				break;
			}
		}
		int dsIndex = 0;
		for (; index < regionWrappers.length; index++, dsIndex++) {
			DataServerWrapper wrapper = dataServerWrappers[dsIndex % dataServerWrappers.length];
			wrapper.capacity += (regionWrappers[index].factor / wrapper.factor);
			if (!loadTargets.containsKey(regionWrappers[index].region)) {
				loadTargets.put(regionWrappers[index].region, wrapper.ds.getAddr());
			}
		}
		return Collections.unmodifiableMap(loadTargets);
	}

	@Override
	public Map<Region, Address> unassignRegion(Collection<DataServerStruct> dataServers) {
		RegionMetric regionMetric = calcRegionMetric(null, dataServers);
		DataServerWrapper[] wrappers = calcDataServerWrappers(dataServers, regionMetric);
		double minCapacity = wrappers[0].capacity;
		double threshCapacity = minCapacity * threshhold;
		for (int i = wrappers.length - 1; i > 0; i--) {
			DataServerWrapper wrapper = wrappers[i];
			if (wrapper.capacity > threshCapacity) {
				Iterator<Region> it = wrapper.ds.getRegions().iterator();
				while (wrapper.capacity > threshCapacity) {
					Region region = it.next();
					wrapper.capacity = wrapper.capacity - calcRegionFactor(region, regionMetric)
							/ wrapper.factor;
					if (!isBusy(region)) {
						unloadTargets.put(region, wrapper.ds.getAddr());
					}
				}
			} else {
				break;
			}
		}

		return Collections.unmodifiableMap(unloadTargets);
	}

	private double calcDataServerFactor(DataServerStruct dataServer, DataServerMetric metric) {
		double memoryFreeDif = (double)dataServer.getInfo().getMemoryFree() / metric.memoryFreeAverage;
		double memoryTotalDif = (double)dataServer.getInfo().getMemoryTotal() / metric.memoryAverage;
		double cpuDif = 10;
		if (dataServer.getInfo().getCpuUsage() > 0) {
			cpuDif = metric.cpuUsageAverage / dataServer.getInfo().getCpuUsage();
		}
		cpuDif = cpuDif > 10 ? 10 : cpuDif;
		double factor = (cpuDif + memoryFreeDif + memoryTotalDif) * dataServer.getWeight() / 3;
		return factor;
	}

	private DataServerMetric calcDataServerMetric(Collection<DataServerStruct> dataServers) {
		long memoryFreeTotal = 0;
		long memoryTotal = 0;
		double cpuUsageTotal = 0;

		for (DataServerStruct ds : dataServers) {
			memoryFreeTotal += ds.getInfo().getMemoryFree();
			memoryTotal += ds.getInfo().getMemoryTotal();
			cpuUsageTotal += ds.getInfo().getCpuUsage();
		}
		long memoryFreeAverage = memoryFreeTotal / dataServers.size();
		long memoryAverage = memoryTotal / dataServers.size();
		double cpuUsageAverage = cpuUsageTotal / dataServers.size();
		return new DataServerMetric(memoryAverage, memoryFreeAverage, cpuUsageAverage);
	}

	private DataServerWrapper[] calcDataServerWrappers(Collection<DataServerStruct> dataServers,
			RegionMetric regionMetric) {
		DataServerMetric dataServerMetric = calcDataServerMetric(dataServers);
		int index = 0;
		DataServerWrapper[] dataServerWrappers = new DataServerWrapper[dataServers.size()];
		for (DataServerStruct dataServer : dataServers) {
			double factor = calcDataServerFactor(dataServer, dataServerMetric);
			double capacity = 0;
			for (Region region : dataServer.getRegions()) {
				capacity += calcRegionFactor(region, regionMetric);
			}
			capacity /= factor;
			dataServerWrappers[index++] = new DataServerWrapper(dataServer, capacity, factor);
		}
		Arrays.sort(dataServerWrappers);
		return dataServerWrappers;
	}

	private double calcRegionFactor(Region region, RegionMetric metric) {
		double sizeDif = (double)region.getStat().size / metric.sizeAverage;
		double readCountDif = (double)region.getStat().readCount / metric.readCountAverage;
		double writeCountDif = (double)region.getStat().writeCount / metric.writeCountAverage;
		return (2 * sizeDif + readCountDif + writeCountDif) / 4;
	}

	private RegionMetric calcRegionMetric(Collection<Region> regions,
			Collection<DataServerStruct> dataServers) {
		long sizeTotal = 0;
		long readCountTotal = 0;
		long writeCountTotal = 0;
		int regionNum = 0;
		if (regions != null) {
			for (Region region : regions) {
				sizeTotal += region.getStat().size;
				readCountTotal += region.getStat().readCount;
				writeCountTotal += region.getStat().writeCount;
				regionNum++;
			}
		}
		if (dataServers != null) {
			for (DataServerStruct ds : dataServers) {
				for (Region region : ds.getRegions()) {
					sizeTotal += region.getStat().size;
					readCountTotal += region.getStat().readCount;
					writeCountTotal += region.getStat().writeCount;
					regionNum++;
				}
			}
		}
		long sizeAverage = sizeTotal / regionNum;
		long readCountAverage = readCountTotal / regionNum;
		long writeCountAverage = writeCountTotal / regionNum;
		return new RegionMetric(sizeAverage, readCountAverage, writeCountAverage);
	}

	private RegionWrapper[] calcRegionWrappers(Collection<Region> regions,
			Collection<DataServerStruct> dataServers, RegionMetric regionMetric) {
		RegionWrapper[] regionWrappers = new RegionWrapper[regions.size()];
		int index = 0;
		for (Region region : regions) {
			double factor = calcRegionFactor(region, regionMetric);
			regionWrappers[index++] = new RegionWrapper(region, factor);
		}
		return regionWrappers;
	}

	private class DataServerMetric {
		public long memoryAverage;
		public long memoryFreeAverage;
		public double cpuUsageAverage;

		public DataServerMetric(long memoryAverage, long memoryFreeAverage, double cpuUsageAverage) {
			super();
			this.memoryAverage = memoryAverage;
			this.memoryFreeAverage = memoryFreeAverage;
			this.cpuUsageAverage = cpuUsageAverage;
		}

	}

	private class DataServerWrapper implements Comparable<DataServerWrapper> {
		public DataServerStruct ds;
		public double capacity;
		public double factor;

		public DataServerWrapper(DataServerStruct ds, double capacity, double factor) {
			this.ds = ds;
			this.capacity = capacity;
			this.factor = factor;
		}

		@Override
		public int compareTo(DataServerWrapper o) {
			return Double.compare(capacity, o.capacity);
		}
	}

	private class RegionMetric {
		public long sizeAverage;
		public long writeCountAverage;
		public long readCountAverage;

		public RegionMetric(long sizeAverage, long writeCountAverage, long readCountAverage) {
			if (sizeAverage < 1) {
				sizeAverage = 1;
			}
			if (writeCountAverage < 1) {
				writeCountAverage = 1;
			}
			if (readCountAverage < 1) {
				readCountAverage = 1;
			}
			this.sizeAverage = sizeAverage;
			this.writeCountAverage = writeCountAverage;
			this.readCountAverage = readCountAverage;
		}

	}

	private class RegionWrapper implements Comparable<RegionWrapper> {
		public Region region;
		public double factor;

		public RegionWrapper(Region region, double factor) {
			this.region = region;
			this.factor = factor;
		}

		@Override
		public int compareTo(RegionWrapper o) {
			return Double.compare(factor, o.factor);
		}

	}
}
