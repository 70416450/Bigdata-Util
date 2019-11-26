package com.bigdata.prometheus.template;

import io.prometheus.client.Collector;

import java.util.List;

/**
* @author Heaton
* @email tzytzy70416450@gmail.com
* @date 2019/8/8 14:37
* @describe 服务控制模板抽象
*/
public abstract class AbstractCollector extends Collector {
    public String labelName;
    public String metricsName;
    public String help;

    /**
    * @param [value -> 需要跟新的数值]
    * @return void
    * @describe 更新监控数值
    */
    public abstract void update(long value);

    /**
    * @param []
    * @return void
    * @describe 重置数值
    */
    public abstract void reset();

    /**
    * @param [value -> 需要充值为的数值]
    * @return void
    * @describe 重置数值为{ value }
    */
    public abstract void reset(long value);


    public abstract List<MetricFamilySamples> collect();
}
