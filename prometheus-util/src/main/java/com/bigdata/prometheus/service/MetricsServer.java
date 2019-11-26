package com.bigdata.prometheus.service;

import com.bigdata.prometheus.template.AbstractCollector;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import lombok.extern.log4j.Log4j;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Heaton
 * @email tzytzy70416450@gmail.com
 * @date 2019/8/8 14:37
 * @describe 监控服务
 */
@Log4j
public class MetricsServer<T extends AbstractCollector> {
    private HTTPServer server;
    private ConcurrentHashMap<String, CollectorInfo> collectors = new ConcurrentHashMap<>();

    private AtomicBoolean started = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("BigdataMetricsThread"));

    private MetricsServer() {

    }

    public static class SingletonHolder {
        private static final MetricsServer SINGLETON = new MetricsServer();
    }

    public static MetricsServer instance() {
        return SingletonHolder.SINGLETON;
    }

    private void initialize() {
        // for jvm
        DefaultExports.initialize();
        Iterator<Map.Entry<String, CollectorInfo>> iterator = collectors.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, CollectorInfo> next = iterator.next();
            String key = next.getKey();
            CollectorInfo value = next.getValue();
            if (value.isInitialized() != true) {
                value.getCollector().register();
                value.setInitialized(true);
                collectors.put(key, value);
            }
        }
    }

    public void start(int port) {
        initialize();
        if (!started.compareAndSet(false, true)) {
            log.warn("Metrics Server already start!");
            return;
        }

        if (collectors.size() <= 0) {
            log.error("Bootstrap failure, collectors is empty!");
            return;
        }
        startHttpService(port);
    }

    public void startHttpService(int port) {
        Objects.requireNonNull(port);
        log.info("Metrics server bind the port " + port + " and starting");
        try {
            server = new HTTPServer(port);
        } catch (IOException e) {
            log.error("MetricsServer start failure!", e);
        }
    }

    public void stop() {
        server.stop();
        scheduledExecutorService.shutdown();
    }

    public void addCollector(T t) {
        Objects.requireNonNull(t.metricsName, "Collector name must not null");
        CollectorInfo collectorInfo = new CollectorInfo(t.metricsName, t);
        collectors.put(t.metricsName, collectorInfo);
    }

    public void addCleaner(String name, long initialDelay, long period, TimeUnit unit) {
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            CollectorInfo collectorInfo = collectors.get(name);
            if (collectorInfo != null) {
                AbstractCollector collector = collectorInfo.getCollector();
                collector.reset();
            }
        }, initialDelay, period, unit);
    }
}
