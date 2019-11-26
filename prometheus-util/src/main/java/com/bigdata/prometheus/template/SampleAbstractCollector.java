package com.bigdata.prometheus.template;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author Heaton
 * @email tzytzy70416450@gmail.com
 * @date 2019/8/8 14:37
 * @describe 简单服务控制模板
 */
@Getter
public abstract class SampleAbstractCollector extends AbstractCollector {
    private List<String> labelList;
    private LongAdder value = new LongAdder();

    public SampleAbstractCollector(String labelName, String metricsName, String help) {
        Objects.requireNonNull(labelName);
        Objects.requireNonNull(metricsName);
        Objects.requireNonNull(help);

        this.labelName = labelName;
        this.metricsName = metricsName;
        this.help = help;
        labelList = Collections.singletonList(labelName);
    }

    @Override
    public void update(long value) {
        Objects.requireNonNull(value);
        ifReset();
        this.value.add(value);
    }



    @Override
    public void reset() {
        this.value.reset();
    }

    @Override
    public void reset(long value) {
        this.value.reset();
        this.value.add(value);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        ArrayList<MetricFamilySamples> list = new ArrayList<>();
        MetricFamilySamples metricFamilySamples = getMetricFamilySamples();
        list.add(metricFamilySamples);
        return list;
    }

    /**
     * @param []
     * @return void
     * @describe 每次更新数值是否重置,不做累加,子类在其中调用reset()方法
     */
    public abstract void ifReset();


    public abstract MetricFamilySamples getMetricFamilySamples();
}
