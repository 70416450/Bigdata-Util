package com.bigdata.prometheus.service;

import com.bigdata.prometheus.template.AbstractCollector;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author Heaton
 * @email tzytzy70416450@gmail.com
 * @date 2019/8/8 14:37
 * @describe 控制器集合信息
 */
@Getter
@Setter
@NoArgsConstructor
public class CollectorInfo<T extends AbstractCollector> {

    private String CollectorName;
    private boolean initialized = false;
    private T collector;

    public CollectorInfo(String collectorName, T collector) {
        CollectorName = collectorName;
        this.collector = collector;
    }
}
