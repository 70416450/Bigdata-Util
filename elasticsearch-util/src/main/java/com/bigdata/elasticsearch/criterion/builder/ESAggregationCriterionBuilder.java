package com.bigdata.elasticsearch.criterion.builder;

import com.bigdata.elasticsearch.criterion.ESAggregationCriterion;
import org.apache.commons.collections4.CollectionUtils;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Heaton
 * @email tzytzy70416450@gmail.com
 * @date 2019/11/15 17:16
 * @describe ES聚合条件构件
 */
public class ESAggregationCriterionBuilder implements ESAggregationCriterion {
    private List<AggregationBuilder> list = new ArrayList<>();

    /**
     * @param [fieldName->分组字段名]
     * @return com.bigdata.elasticsearch.criterion.builder.ESAggregationCriterionBuilder
     * @describe 分组条件
     */
    public ESAggregationCriterionBuilder group(String fieldName) {
        list.add(AggregationBuilders.terms(fieldName).field(fieldName));
        return this;
    }

    /**
     * @param [fieldName->分组字段名]
     * @return com.bigdata.elasticsearch.criterion.builder.ESAggregationCriterionBuilder
     * @describe 最大值条件
     */
    public ESAggregationCriterionBuilder max(String fieldName) {
        list.add(AggregationBuilders.max("agg").field(fieldName));
        return this;
    }

    /**
     * @param [fieldName->分组字段名]
     * @return com.bigdata.elasticsearch.criterion.builder.ESAggregationCriterionBuilder
     * @describe 最小值条件
     */
    public ESAggregationCriterionBuilder min(String fieldName) {
        list.add(AggregationBuilders.min("agg").field(fieldName));
        return this;
    }

    /**
     * @param [fieldName->分组字段名]
     * @return com.bigdata.elasticsearch.criterion.builder.ESAggregationCriterionBuilder
     * @describe 平均值条件
     */
    public ESAggregationCriterionBuilder avg(String fieldName) {
        list.add(AggregationBuilders.avg("agg").field(fieldName));
        return this;
    }

    /**
     * @param [fieldName->分组字段名]
     * @return com.bigdata.elasticsearch.criterion.builder.ESAggregationCriterionBuilder
     * @describe 求和条件
     */
    public ESAggregationCriterionBuilder sum(String fieldName) {
        list.add(AggregationBuilders.sum("agg").field(fieldName));
        return this;
    }

    @Override
    public AggregationBuilder builder() {
        if (!CollectionUtils.isEmpty(list)) {
            AggregationBuilder aggregationBuilder = list.get(0);
            for (int i = 1; i < list.size(); i++) {
                aggregationBuilder.subAggregation(list.get(i));
            }
            return aggregationBuilder;
        }
        return null;
    }
}
