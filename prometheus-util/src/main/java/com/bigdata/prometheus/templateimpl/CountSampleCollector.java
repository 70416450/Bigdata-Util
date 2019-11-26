package com.bigdata.prometheus.templateimpl;

import com.bigdata.prometheus.template.SampleAbstractCollector;
import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;

import java.util.Collections;

/**
 * @author Heaton
 * @email tzytzy70416450@gmail.com
 * @date 2019/8/8 14:37
 * @describe 记总数的控制器
 */
public class CountSampleCollector extends SampleAbstractCollector {

    public CountSampleCollector(String labelName, String metricsName, String help) {
        super(labelName, metricsName, help);
    }

    //累加不重置
    @Override
    public void ifReset() {
    }

    @Override
    public Collector.MetricFamilySamples getMetricFamilySamples() {
        CounterMetricFamily counter = new CounterMetricFamily(metricsName, help, getLabelList());
        return counter.addMetric(Collections.singletonList(metricsName), getValue().sum());
    }
}
