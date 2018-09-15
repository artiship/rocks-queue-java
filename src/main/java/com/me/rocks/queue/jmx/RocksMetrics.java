package com.me.rocks.queue.jmx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

public abstract class RocksMetrics {
    private static final Logger log = LoggerFactory.getLogger(RocksMetrics.class);
    private ObjectName name;

    public void register() {
        try {
            this.name = new ObjectName(this.getObjectName());
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            mBeanServer.registerMBean(this, name);
        } catch (JMException e) {
            log.warn("Error while register the MBean '{}': {}", name, e.getMessage());
        }
    }

    public void unregister() {
        if (this.name != null) {
            try {
                MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                mBeanServer.unregisterMBean(name);
            } catch (JMException e) {
                log.error("Unable to unregister the MBean '{}'", name);
            } finally {
                this.name = null;
            }
        }
    }

    protected abstract String getObjectName();

    protected long getCurrentTimeMillis() {
        return System.currentTimeMillis();
    }
}
