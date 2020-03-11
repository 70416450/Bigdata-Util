package com.bigdata.elasticsearch.criterion.manager;


import com.bigdata.elasticsearch.criterion.ESQueryCriterion;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Heaton
 * @email tzytzy70416450@gmail.com
 * @date 2019/11/15 17:16
 * @describe ES查询条件最终构建者
 */
@Getter
@Setter
public class ESQueryCriterionBuilderManager {

    private int size = Integer.MAX_VALUE;

    private int from = 0;

    private String asc;

    private String desc;

    //查询条件容器
    private List<ESQueryCriterion> mustCriterions = new ArrayList<>();
    private List<ESQueryCriterion> shouldCriterions = new ArrayList<>();
    private List<ESQueryCriterion> mustNotCriterions = new ArrayList<>();

    /** 
    * @param [criterion->必须条件集合]
    * @return com.bigdata.elasticsearch.criterion.manager.ESQueryCriterionBuilderManager
    * @describe 必须条件封装
    */
    public ESQueryCriterionBuilderManager must(ESQueryCriterion criterion) {
        if (criterion != null) {
            mustCriterions.add(criterion);
        }
        return this;
    }
    
    /** 
    * @param [criterion->或者条件集合]
    * @return com.bigdata.elasticsearch.criterion.manager.ESQueryCriterionBuilderManager
    * @describe 或者条件封装
    */
    public ESQueryCriterionBuilderManager should(ESQueryCriterion criterion) {
        if (criterion != null) {
            shouldCriterions.add(criterion);
        }
        return this;
    }

    /** 
    * @param [criterion->必须不条件集合]
    * @return com.bigdata.elasticsearch.criterion.manager.ESQueryCriterionBuilderManager
    * @describe 必须不条件封装
    */
    public ESQueryCriterionBuilderManager mustNot(ESQueryCriterion criterion) {
        if (criterion != null) {
            mustNotCriterions.add(criterion);
        }
        return this;
    }


    /**
     * @return org.elasticsearch.index.query.QueryBuilder
     * @describe 条件封装完毕后调用build构造ES的查询对象体
     */
    public QueryBuilder build() {
        int count = mustCriterions.size() + shouldCriterions.size() + mustNotCriterions.size();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        if (count >= 1) {
            //must容器
            if (!CollectionUtils.isEmpty(mustCriterions)) {
                for (ESQueryCriterion criterion : mustCriterions) {
                    for (QueryBuilder builder : criterion.listBuilders()) {
                        boolQueryBuilder = boolQueryBuilder.must(builder);

                    }
                }
            }
            //should容器
            if (!CollectionUtils.isEmpty(shouldCriterions)) {
                for (ESQueryCriterion criterion : shouldCriterions) {
                    for (QueryBuilder builder : criterion.listBuilders()) {
                        boolQueryBuilder = boolQueryBuilder.should(builder);
                    }

                }
            }
            //must not 容器
            if (!CollectionUtils.isEmpty(mustNotCriterions)) {
                for (ESQueryCriterion criterion : mustNotCriterions) {
                    for (QueryBuilder builder : criterion.listBuilders()) {
                        boolQueryBuilder = boolQueryBuilder.mustNot(builder);
                    }
                }
            }
            return boolQueryBuilder;
        } else {
            return null;
        }
    }
}