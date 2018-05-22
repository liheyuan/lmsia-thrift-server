package com.coder4.lmsia.thrift.server.configuration;

import com.coder4.lmsia.thrift.server.ThriftServerRunnable;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author coder4
 */
@Configuration
@ConditionalOnBean(value = {TProcessor.class})
public class ThriftServerConfiguration implements InitializingBean, DisposableBean {

    private Logger LOG = LoggerFactory.getLogger(ThriftServerConfiguration.class);

    @Autowired
    private TProcessor processor;

    private ThriftServerRunnable thriftServer;

    private Thread thread;

    @Bean(name = "shutdownThriftServerRunnable")
    public Runnable shutdownThriftServerRunnable() {
        return () -> {
            LOG.info("Shutdown thrift server.");
            try {
                thriftServer.stop();
            } catch (Exception e) {
                LOG.error("Shutdown thrift server error", e);
            }
        };
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        thriftServer = new ThriftServerRunnable(processor);
        thread = new Thread(thriftServer);
        thread.start();
    }

    @Override
    public void destroy() throws Exception {
        try {
            thread.join();
            LOG.info("Join thrift server thread done.");
        } catch (Exception e) {
            LOG.error("Join thrift server thread error", e);
        }
    }
}
