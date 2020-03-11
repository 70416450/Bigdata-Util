package com.bigdata.elasticsearch.pojo;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Collection;

import static com.bigdata.elasticsearch.criterion.ESQueryCriterion.Operator;

/**
 * @author Heaton
 * @email tzytzy70416450@gmail.com
 * @date 2019/11/15 17:16
 * @describe ES查询条件实体类
 */
public class ESQueryPojo {
    //属性名
	private String fieldName;
    //对应值
    private Object value;
    //对应值集合
    private Collection<Object> values;
    //计算符
    private Operator operator;
    private Object from;
    private Object to;

    public ESQueryPojo(String fieldName, Object value, Operator operator) {
        this.fieldName = fieldName;
        this.value = value;
        this.operator = operator;
    }

    public ESQueryPojo(String value, Operator operator) {
        this.value = value;
        this.operator = operator;
    }

    public ESQueryPojo(String fieldName, Collection<Object> values) {
        this.fieldName = fieldName;
        this.values = values;
        this.operator = Operator.TERMS;
    }

    public ESQueryPojo(String fieldName, Object from, Object to) {
        this.fieldName = fieldName;
        this.from = from;
        this.to = to;
        this.operator = Operator.RANGE;
    }

    public QueryBuilder toQueryBuilder() {
        QueryBuilder qb = null;
        switch (operator) {
            case TERM:
                qb = QueryBuilders.termQuery(fieldName, value);
                break;
            case TERMS:
                qb = QueryBuilders.termsQuery(fieldName, values);
                break;
            case RANGE:
                qb = QueryBuilders.rangeQuery(fieldName).from(from).to(to).includeLower(true).includeUpper(true);
                break;
            case FUZZY:
                qb = QueryBuilders.fuzzyQuery(fieldName, value);
                break;
            case QUERY_STRING:
                qb = QueryBuilders.queryStringQuery(value.toString());
        }
        return qb;
    }
}