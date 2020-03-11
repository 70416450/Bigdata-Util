package com.bigdata.elasticsearch.criterion.builder;

import com.bigdata.elasticsearch.pojo.ESQueryPojo;
import com.bigdata.elasticsearch.criterion.ESQueryCriterion;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * @author Heaton
 * @email tzytzy70416450@gmail.com
 * @date 2019/11/15 17:16
 * @describe ES查询条件构件
 */
public class ESQueryCriterionBuilder implements ESQueryCriterion {

    private List<QueryBuilder> list = new ArrayList<>();

    /**
    * @param [field->字段名, value->字段值]
    * @return com.bigdata.elasticsearch.criterion.builder.ESQueryCriterionBuilder
    * @describe Term 精确查询
    */
    public ESQueryCriterionBuilder term(String field, Object value) {
        list.add(new ESQueryPojo(field, value, Operator.TERM).toQueryBuilder());
        return this;
    }

    /** 
    * @param [field->字段名, values->字段集合值]
    * @return com.bigdata.elasticsearch.criterion.builder.ESQueryCriterionBuilder 
    * @describe Terms 精确查询
    */
    public ESQueryCriterionBuilder term(String field, Collection<Object> values) {
        list.add(new ESQueryPojo(field, values).toQueryBuilder());
        return this;
    }

    /**
    * @param [field->字段名, values->字段集合值]
    * @return com.bigdata.elasticsearch.criterion.builder.ESQueryCriterionBuilder
    * @describe fuzzy 模糊查询
    */
    public ESQueryCriterionBuilder fuzzy(String field, Object value) {
        list.add(new ESQueryPojo(field, value, Operator.FUZZY).toQueryBuilder());
        return this;
    }

    /**
    * @param [field->字段名 , from->起始值 , to->末尾值 ]
    * @return com.bigdata.elasticsearch.criterion.builder.ESQueryCriterionBuilder 
    * @describe Range 范围查询
    */
    public ESQueryCriterionBuilder range(String field, Object from, Object to) {
        list.add(new ESQueryPojo(field, from, to).toQueryBuilder());
        return this;
    }

    /** 
    * @param [queryString->查询语句]
    * @return com.bigdata.elasticsearch.criterion.builder.ESQueryCriterionBuilder 
    * @describe queryString查询
    */
    public ESQueryCriterionBuilder queryString(String queryString) {
        list.add(new ESQueryPojo(queryString, Operator.QUERY_STRING).toQueryBuilder());
        return this;
    }

    public List<QueryBuilder> listBuilders() {
        return list;
    }
}