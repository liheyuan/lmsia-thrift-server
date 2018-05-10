package com.coder4.lmsia.thrift.server.configuration;

import com.coder4.lmsia.thrift.server.ThriftServerRunnable;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * @author coder4
 */
@Configuration
@ConditionalOnBean(value = {TProcessor.class})
public class ThriftServerConfiguration implements InitializingBean, DisposableBean {

    private Logger LOG = LoggerFactory.getLogger(ThriftServerConfiguration.class);

    private static final int GRACEFUL_SHOWDOWN_SEC = 3;

    @Autowired
    private TProcessor processor;

    private ThriftServerRunnable thriftServer;

    private Thread thread;

    @Override
    public void destroy() throws Exception {
        // TODO graceful shutdown

        LOG.info("Wait for graceful shutdown on destroy(), {} seconds", GRACEFUL_SHOWDOWN_SEC);
        Thread.sleep(TimeUnit.SECONDS.toMillis(GRACEFUL_SHOWDOWN_SEC));
        LOG.info("Shutdown rpc server.");
        thriftServer.stop();
        thread.join();
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        thriftServer = new ThriftServerRunnable(processor);
        thread = new Thread(thriftServer);
        thread.start();
    }
}
