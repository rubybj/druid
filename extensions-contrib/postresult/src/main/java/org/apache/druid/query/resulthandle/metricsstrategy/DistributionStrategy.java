/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.druid.query.resulthandle.metricsstrategy;


import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.Result;
import org.apache.druid.query.resulthandle.DruidResultSecondDevHandler;
import org.apache.druid.query.resulthandle.tool.ResultConst;
import org.apache.druid.query.topn.DimensionAndMetricValueExtractor;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.query.resulthandle.tool.Buckets;
import org.apache.druid.query.resulthandle.tool.ResultChange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DistributionStrategy implements IMetricsStrategy {
    protected static final EmittingLogger LOG = new EmittingLogger(DistributionStrategy.class);
    @Override
    public void topNDataHandle(Map<String,Object> values) {

        Map resultMap=  DruidResultSecondDevHandler.getInstance().getThreadLocal().get();

        try {
            String metric = (String) resultMap.get(ResultConst.METRIC);
               // String dimension_type = (String) resultModel.get("dimension_type");
                String dimension = (String) resultMap.get(ResultConst.DIMENSION);
                String name = (String) resultMap.get(ResultConst.NAME);
                int[] buckets=(int[])resultMap.get(ResultConst.BUCKETS);
                String[] bucketDes = (String[])resultMap.get(ResultConst.BUCKETSDES);
                int[] buckets_count = (int[])resultMap.get(ResultConst.BUCKETSCOUNT);
                List<DimensionAndMetricValueExtractor> ldNew = new ArrayList<DimensionAndMetricValueExtractor>();
                int other_num=(int)resultMap.get(ResultConst.OTHERNUM);

                Object metric_num = values.get(metric);
                    try{
                        metric_num=((Number) metric_num).intValue();
                    }catch (Exception e){
                    //    e.printStackTrace();
                        metric_num=0;
                    }
                    int metric_n=((Number) metric_num).intValue();


                    Double dimension_value ;
                    try {
                        dimension_value = Double.valueOf(values.get(dimension).toString());
                        Buckets.putDataToBuckets(buckets, dimension_value, buckets_count, metric_n);
                    }catch (Exception e){
                  //      e.printStackTrace();
                        other_num=other_num+metric_n;
                        resultMap.put(ResultConst.OTHERNUM,other_num);
                    }
/*

                */
            }catch (Exception e){

        }

        /*
        Sequence s = Sequences.simple(al);
        long a=System.currentTimeMillis();
        long aa=a-t;
        return Yielders.each(s);
         */
        return ;
    }
    @Override
    public void groupByDataHandle(Map<String,Object> values){
      /**
        long t=System.currentTimeMillis();
        ResultChange resultChange = new ResultChange();
        Yielder y =resultChange.GroupByResultToTopNResult(yielder);
        long a=System.currentTimeMillis();
        LOG.info("结果集转换耗时======"+(a-t));
        return topNDataHandle(y,resultModel);
       **/
      return ;
    }

}
