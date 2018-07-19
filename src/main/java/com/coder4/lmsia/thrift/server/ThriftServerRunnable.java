package com.coder4.lmsia.thrift.server;

import com.coder4.sbmvt.thrift.common.TraceBinaryProtocol.Factory;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.coder4.sbmvt.thrift.common.Constants.THRIFT_CORE_THREADS;
import static com.coder4.sbmvt.thrift.common.Constants.THRIFT_MAX_FRAME_SIZE;
import static com.coder4.sbmvt.thrift.common.Constants.THRIFT_MAX_READ_BUF_SIZE;
import static com.coder4.sbmvt.thrift.common.Constants.THRIFT_MAX_THREADS;
import static com.coder4.sbmvt.thrift.common.Constants.THRIFT_PORT;
import static com.coder4.sbmvt.thrift.common.Constants.THRIFT_SELECTOR_THREADS;
import static com.coder4.sbmvt.thrift.common.Constants.THRIFT_TCP_BACKLOG;
import static com.coder4.sbmvt.thrift.common.Constants.THRIFT_TIMEOUT;

/**
 * @author coder4
 */
public class ThriftServerRunnable implements Runnable {

    private static final Factory THRIFT_PROTOCOL_FACTORY = new Factory();

    protected ExecutorService threadPool;

    protected TServer server;

    private TProcessor processor;

    public ThriftServerRunnable(TProcessor processor) {
        this.processor = processor;
    }

    public TServer build() throws TTransportException {
        TNonblockingServerSocket.NonblockingAbstractServerSocketArgs socketArgs =
                new TNonblockingServerSocket.NonblockingAbstractServerSocketArgs();
        socketArgs.port(THRIFT_PORT);
        socketArgs.clientTimeout(THRIFT_TIMEOUT);
        socketArgs.backlog(THRIFT_TCP_BACKLOG);

        TNonblockingServerTransport transport = new TNonblockingServerSocket(socketArgs);

        threadPool =
                new ThreadPoolExecutor(THRIFT_CORE_THREADS, THRIFT_MAX_THREADS,
                        60L, TimeUnit.SECONDS,
                        new SynchronousQueue<>());

        TTransportFactory transportFactory = new TFramedTransport.Factory(THRIFT_MAX_FRAME_SIZE);
        TThreadedSelectorServer.Args args = new TThreadedSelectorServer.Args(transport)
                .selectorThreads(THRIFT_SELECTOR_THREADS)
                .executorService(threadPool)
                .transportFactory(transportFactory)
                .inputProtocolFactory(THRIFT_PROTOCOL_FACTORY)
                .outputProtocolFactory(THRIFT_PROTOCOL_FACTORY)
                .processor(processor);

        args.maxReadBufferBytes = THRIFT_MAX_READ_BUF_SIZE;

        return new TThreadedSelectorServer(args);
    }

    @Override
    public void run() {
        try {
            server = build();
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Start Thrift RPC Server Exception");
        }
    }

    public void stop() throws Exception {
        threadPool.shutdown();
        server.stop();
    }

}