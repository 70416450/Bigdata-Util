package com.bigdata.prometheus.templateimpl;

import com.bigdata.prometheus.template.SampleAbstractCollector;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;

import java.util.Collections;

/**
 * @author Heaton
 * @email tzytzy70416450@gmail.com
 * @date 2019/8/8 14:37
 * @describe 记每次数量的控制器
 */
public class EntrySampleCollector extends SampleAbstractCollector {
    public EntrySampleCollector(String labelName, String metricsName, String help) {
        super(labelName, metricsName, help);
    }
    //重置
    @Override
    public void ifReset() {
        reset();
    }

    @Override
    public Collector.MetricFamilySamples getMetricFamilySamples() {
        GaugeMetricFamily entryGauge = new GaugeMetricFamily(metricsName, help, getLabelList());
        return entryGauge.addMetric(Collections.singletonList(metricsName), getValue().sum());
    }
}
