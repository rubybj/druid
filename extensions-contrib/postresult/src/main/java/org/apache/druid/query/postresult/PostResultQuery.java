package org.apache.druid.query.postresult;/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.*;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.TopNQuery;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * MaterializedViewQuery helps to do materialized view selection automatically.
 *
 * Each MaterializedViewQuery contains a real query which type can be topn, timeseries or groupBy.
 * The real query will be optimized based on its dataSources and intervals. It will be converted into one or more
 * sub-queries, in which dataSources and intervals are replaced by derived dataSources and related sub-intervals.
 *
 * Derived dataSources always have less dimensions, but contains all dimensions which real query required.
 */
public class PostResultQuery<T> implements Query<T>
{
  public static final String TYPE = "postresult";
  private final Query query;
  private  Map<String,Object> resultContext=new HashMap<String,Object>();
 // private final DataSourceOptimizer optimizer;
  
  @JsonCreator
  public  PostResultQuery(
      @JsonProperty("query") Query query
  )
  {
    Preconditions.checkArgument(
        query instanceof TopNQuery || query instanceof TimeseriesQuery || query instanceof GroupByQuery,
        "Only topN/timeseries/groupby query are supported"
    );
    this.query = query;
  }

  public  PostResultQuery(
          Query query,Map<String,Object> context)
  {
    Preconditions.checkArgument(
            query instanceof TopNQuery || query instanceof TimeseriesQuery || query instanceof GroupByQuery,
            "Only topN/timeseries/groupby query are supported"
    );
    this.query = query;
    resultContext=context;
  }
  
  @JsonProperty("query")
  public Query getQuery()
  {
    return query;
  }

  
  @Override
  public DataSource getDataSource()
  {
    return query.getDataSource();
  }
  
  @Override
  public boolean hasFilters()
  {
    return query.hasFilters();
  }

  @Override
  public DimFilter getFilter()
  {
    return query.getFilter();
  }

  @Override
  public String getType()

  {
    return TYPE;
    //return query.getType();
  }

  @Override
  public QueryRunner<T> getRunner(QuerySegmentWalker walker) 
  {
    if(query.getContext().containsKey("result_type")){
      resultContext.putAll(query.getContext());
      query.getContext().remove("result_type");
      query.getContext().remove("result_model");
    }
    return ((BaseQuery) query).getQuerySegmentSpec().lookup(this, walker);
  }

  @Override
  public List<Interval> getIntervals()
      
  {
    return query.getIntervals();
  }

  @Override
  public Duration getDuration()
  {
    return query.getDuration();
  }

  @Override
  public Granularity getGranularity()
  {
    return query.getGranularity();
  }

  @Override
  public DateTimeZone getTimezone()
  {
    return query.getTimezone();
  }

  @Override
  public Map<String, Object> getContext()
  {

    return query.getContext();
  }

  public Map<String, Object> getContextWithResult()
  {
    return resultContext;
  }

  @Override
  public <ContextType> ContextType getContextValue(String key)
  {
    return (ContextType) query.getContextValue(key);
  }

  @Override
  public <ContextType> ContextType getContextValue(String key, ContextType defaultValue) 
  {
    return (ContextType) query.getContextValue(key, defaultValue);
  }

  @Override
  public boolean getContextBoolean(String key, boolean defaultValue) 
  {
    return query.getContextBoolean(key, defaultValue);
  }

  @Override
  public boolean isDescending()
  {
    return query.isDescending();
  }

  @Override
  public Ordering<T> getResultOrdering()
  {
    return query.getResultOrdering();
  }

  @Override
  public PostResultQuery withOverriddenContext(Map<String, Object> contextOverride)
  {


    return new PostResultQuery(query.withOverriddenContext(contextOverride),getContextWithResult());
  }

  @Override
  public PostResultQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new PostResultQuery(query.withQuerySegmentSpec(spec),getContextWithResult());
  }

  @Override
  public  PostResultQuery withId(String id)
  {
    return new  PostResultQuery(query.withId(id),getContextWithResult());
  }

  @Override
  public String getId()
  {
    return query.getId();
  }

  @Override
  public Query<T> withSubQueryId(String subQueryId)
  {
    return new  PostResultQuery<>(query.withSubQueryId(subQueryId));
  }

  @Nullable
  @Override
  public String getSubQueryId()
  {
    return query.getSubQueryId();
  }

  @Override
  public PostResultQuery withDataSource(DataSource dataSource)
  {
    return new PostResultQuery(query.withDataSource(dataSource),getContextWithResult());
  }

  @Override
  public String toString()
  {
    return "PostresultQuery{" +
           "query=" + query +
           "}";
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
     PostResultQuery other = (PostResultQuery) o;
    return other.getQuery().equals(query);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(TYPE, query);
  }

}
