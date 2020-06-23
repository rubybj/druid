/*
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

package org.apache.druid.query.postresult;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.sun.media.jfxmedia.logging.Logger;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.*;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.resulthandle.DruidResultSecondDevHandler;
import org.apache.druid.query.topn.DimensionAndMetricValueExtractor;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.server.QueryResource;

import java.util.*;
import java.util.function.BinaryOperator;

public class PostQueryToolChest extends QueryToolChest
{
  private final QueryToolChestWarehouse warehouse;
  protected static final EmittingLogger log = new EmittingLogger(PostQueryToolChest.class);
 // private DataSourceOptimizer optimizer;

  @Inject
  public PostQueryToolChest(
      QueryToolChestWarehouse warehouse
  )
  {
    this.warehouse = warehouse;
  }
  
  @Override
  public QueryRunner mergeResults(QueryRunner runner)
  {
    return new QueryRunner() {
      @Override
      public Sequence run(QueryPlus queryPlus, ResponseContext responseContext)
      {
        Query realQuery = getRealQuery(queryPlus.getQuery());
        return warehouse.getToolChest(realQuery).mergeResults(runner).run(queryPlus.withQuery(realQuery), responseContext);
      }
    };
  }

  @Override
  public BinaryOperator createMergeFn(Query query)
  {
    final Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).createMergeFn(realQuery);
  }

  @Override
  public Comparator createResultComparator(Query query)
  {
    final Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).createResultComparator(realQuery);
  }

  @Override
  public QueryMetrics makeMetrics(Query query) 
  {
    Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).makeMetrics(realQuery);
  }

  @Override
  public ObjectMapper decorateObjectMapper(final ObjectMapper objectMapper, final  Query query){

    final Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).decorateObjectMapper(objectMapper,realQuery);

  }

  /**
   *
   * @param query The Query that is currently being processed
   * @param fn    The function that should be applied to all metrics in the results
   *在执行完postagg后，执行粪桶，为了减少不必要的循环，之后后资源被释放。
   * @return
   */
  @Override
  public Function makePreComputeManipulatorFn(Query query, MetricManipulationFn fn) 
  {
    Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).makePreComputeManipulatorFn(realQuery, fn);



  }

  @Override
  public Function makePostComputeManipulatorFn(Query query, MetricManipulationFn fn)
  {
    Query realQuery = getRealQuery(query);
    String type=realQuery.getType();
    String resultType=(String)((PostResultQuery)query).getContextWithResult().get("result_type");
    LinkedHashMap resultModel=(LinkedHashMap)((PostResultQuery)query).getContextWithResult().get("result_model");



    switch (type) {
      case "topN":
        return makePostTOPComputeManipulatorFn_topN((TopNQuery) realQuery, fn,resultType,resultModel);
      default:
        return warehouse.getToolChest(realQuery).makePostComputeManipulatorFn(realQuery, fn);
    }
  }

  @Override
  public QueryRunner postMergeQueryDecoration(QueryRunner runner){
    return new QueryRunner() {
      @Override
      public Sequence run(QueryPlus queryPlus, ResponseContext responseContext)
      {
        Query realQuery = getRealQuery(queryPlus.getQuery());
        return warehouse.getToolChest(realQuery).postMergeQueryDecoration(runner).run(queryPlus.withQuery(realQuery), responseContext);
      }
    };
  }

  @Override
  public TypeReference getResultTypeReference()
  {
    return null;
  }

  public Query getRealQuery(Query query)
  {
    if (query instanceof PostResultQuery) {
      return ((PostResultQuery) query).getQuery();
    }
    return query;
  }
/***ADD BY WANGHBK*/

public Function<Result<TopNResultValue>, Result<TopNResultValue>> makePostTOPComputeManipulatorFn_topN(
        final TopNQuery query,
        final MetricManipulationFn fn,
        final String resultType,
        final HashMap resultModel
)
{

  return new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
  {
    private String dimension = query.getDimensionSpec().getOutputName();
    private final AggregatorFactory[] aggregatorFactories = query.getAggregatorSpecs()
            .toArray(new AggregatorFactory[0]);
    private final String[] aggFactoryNames = extractFactoryName(query.getAggregatorSpecs());
    private final PostAggregator[] postAggregators = query.getPostAggregatorSpecs().toArray(new PostAggregator[0]);



    @Override
    public Result<TopNResultValue> apply(Result<TopNResultValue> result)
    {

      List<Map<String, Object>> serializedValues=null;
      try {
        serializedValues = Lists.newArrayList(
                Iterables.transform(
                        result.getValue(),
                        new Function<DimensionAndMetricValueExtractor, Map<String, Object>>() {
                          @Override
                          public Map<String, Object> apply(DimensionAndMetricValueExtractor input) {
                            final Map<String, Object> values = Maps.newHashMapWithExpectedSize(
                                    aggregatorFactories.length
                                            + query.getPostAggregatorSpecs().size()
                                            + 1
                            );

                            // Put non-finalized aggregators before post-aggregators.
                            for (final String name : aggFactoryNames) {
                              values.put(name, input.getMetric(name));
                            }

                            // Put dimension, post-aggregators might depend on it.
                            values.put(dimension, input.getDimensionValue(dimension));

                            // Put post-aggregators.
                            for (PostAggregator postAgg : postAggregators) {
                              Object calculatedPostAgg = input.getMetric(postAgg.getName());
                              if (calculatedPostAgg != null) {
                                values.put(postAgg.getName(), calculatedPostAgg);
                              } else {
                                values.put(postAgg.getName(), postAgg.compute(values));
                              }
                            }

                            // Put finalized aggregators now that post-aggregators are done.
                            for (int i = 0; i < aggFactoryNames.length; ++i) {
                              final String name = aggFactoryNames[i];
                              values.put(name, fn.manipulate(aggregatorFactories[i], input.getMetric(name)));
                            }
                            DruidResultSecondDevHandler.getInstance().secondDevResultHandler(values, query, resultType, resultModel);

                            return values;
                          }
                        }
                )
        );
        List resultlist= DruidResultSecondDevHandler.getInstance().getResult();
        if (result != null) {
          serializedValues =resultlist;
        }
      }catch (Exception e){
        log.warn("result handle error",e);
      }finally {
        DruidResultSecondDevHandler.getInstance().getThreadLocal().remove();
      }
      return new Result<>(
              result.getTimestamp(),
              new TopNResultValue(serializedValues)
      );
    }
  };
}
  protected static String[] extractFactoryName(final List<AggregatorFactory> aggregatorFactories)
  {
    return aggregatorFactories.stream().map(AggregatorFactory::getName).toArray(String[]::new);
  }
}
