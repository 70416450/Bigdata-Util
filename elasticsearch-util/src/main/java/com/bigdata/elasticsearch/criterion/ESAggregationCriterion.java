package com.bigdata.elasticsearch.criterion;

import org.elasticsearch.search.aggregations.AggregationBuilder;

/**
 * @author Heaton
 * @email tzytzy70416450@gmail.com
 * @date 2019/11/15 17:16
 * @describe ES聚合条件接口
 */
public interface ESAggregationCriterion {

    enum Operator {
        MAX, MIN, AVG, SUM,
    }

    AggregationBuilder builder();
}
