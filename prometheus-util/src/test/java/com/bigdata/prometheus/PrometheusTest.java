package com.bigdata.prometheus;

import com.bigdata.prometheus.service.MetricsServer;
import com.bigdata.prometheus.templateimpl.CountSampleCollector;
import com.bigdata.prometheus.templateimpl.EntrySampleCollector;

import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class PrometheusTest {
    public static void main(String[] args) {

        MetricsServer metricsServer = MetricsServer.instance();
        CountSampleCollector counter = new CountSampleCollector("testPrometheus", "Prometheus_count", "number of records of testPrometheus");
        EntrySampleCollector entry = new EntrySampleCollector("testPrometheus", "Prometheus_entry", "real time records of testPrometheus");

        ArrayList<Object> aa = new ArrayList<>(50);
        metricsServer.addCollector(entry);
        metricsServer.addCollector(counter);
        metricsServer.addCleaner("Prometheus_count", 1000, 1000 * 60, TimeUnit.MILLISECONDS);
        metricsServer.start(22222);

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        entry.update(aa.size());
                        counter.update(aa.size());

                        System.out.println(new Date().toString() + "__________" + aa.size());
                        Thread.sleep(1000 * 15);
                        aa.clear();
                        for (int i = 0; i < new Random().nextInt(50); i++) {
                            aa.add(new Object());
                        }

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }


}