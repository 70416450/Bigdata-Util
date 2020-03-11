package com.bigdata.elasticsearch.criterion;

import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;

/**
 * @author Heaton
 * @email tzytzy70416450@gmail.com
 * @date 2019/11/15 17:16
 * @describe ES查询条件接口
 */
public interface ESQueryCriterion {
    enum Operator {
        TERM, TERMS, RANGE, FUZZY, QUERY_STRING
    }

    List<QueryBuilder> listBuilders();
}