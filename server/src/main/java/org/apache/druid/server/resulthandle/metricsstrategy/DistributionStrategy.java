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
package org.apache.druid.server.resulthandle.metricsstrategy;


import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.Result;
import org.apache.druid.query.topn.DimensionAndMetricValueExtractor;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.server.resulthandle.tool.Buckets;
import org.apache.druid.server.resulthandle.tool.ResultChange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DistributionStrategy implements IMetricsStrategy{
    protected static final EmittingLogger LOG = new EmittingLogger(DistributionStrategy.class);
    @Override
    public Yielder topNDataHandle(Yielder yielder, HashMap resultModel) {
        long t=System.currentTimeMillis();
        String dimension = (String) resultModel.get("dimension");
        String dimension_type = (String) resultModel.get("dimension_type");
        String metric = (String) resultModel.get("metric");
        String name = (String) resultModel.get("name");
        String other = (String) resultModel.get("other");
        Integer bucketMetricFix = (Integer) resultModel.get("bucketMetricFix");
        String bucketMetricFree = (String) resultModel.get("bucketMetricFree");
        String bucketMetricDescrip = (String) resultModel.get("bucketMetricDescrip");

        int size = 100;
        int bucketMetric = 100;
        ArrayList<Result> al = new ArrayList<Result>();
        try {
            while (!yielder.isDone()) {
                int[] buckets;
                String[] bucketDes = null;
                int other_num=0;
                //产生数据桶
                if (bucketMetricFree != null && !"".equals(bucketMetricFree)) {
                    buckets = Buckets.createBuckets(bucketMetricFree);
                } else if (bucketMetricFix != null && !"".equals(bucketMetricFix)) {
                    buckets = Buckets.createBuckets(size, bucketMetricFix);
                } else {
                    buckets = Buckets.createBuckets(size, bucketMetric);
                }
                //产生描述的桶
                if (bucketMetricDescrip != null && !"".equals(bucketMetricDescrip)) {
                    bucketDes = bucketMetricDescrip.split(",");
                }
                int[] buckets_count = new int[buckets.length];
                List<DimensionAndMetricValueExtractor> ld = ((TopNResultValue) ((Result) yielder.get()).getValue()).getValue();
                List<DimensionAndMetricValueExtractor> ldNew = new ArrayList<DimensionAndMetricValueExtractor>();
                for (DimensionAndMetricValueExtractor dm : ld) {

                    Object metric_num = dm.getMetric(metric);
                    try{
                        metric_num=((Number) metric_num).intValue();
                    }catch (Exception e){
                    //    e.printStackTrace();
                        metric_num=0;
                    }
                    int metric_n=((Number) metric_num).intValue();


                    Double dimension_value = null;
                    try {
                        if (("String").equals(dimension_type)) {
                            dimension_value = Double.valueOf(dm.getMetric(dimension).toString());
                        } else if (("double").equals(dimension_type)) {
                            dimension_value = dm.getDoubleMetric(dimension);
                        } else if (("long").equals(dimension_type)) {
                            dimension_value = (double) dm.getLongMetric(dimension);
                        } else {
                            dimension_value = Double.valueOf(dm.getMetric(dimension).toString());
                        }
                        Buckets.putDataToBuckets(buckets, dimension_value, buckets_count, metric_n);
                    }catch (Exception e){
                  //      e.printStackTrace();
                        other_num=other_num+metric_n;
                    }
                }
                for (int i = 0; i < buckets_count.length - 1; i++) {
                    Map<String, Object> map = new HashMap();
                    if (bucketDes != null) {
                        map.put(dimension, bucketDes[i]);
                    } else {
                        map.put(dimension, buckets[i] + "-" + buckets[i + 1]);
                    }
                    map.put(name, buckets_count[i]);
                    DimensionAndMetricValueExtractor dmve = new DimensionAndMetricValueExtractor(map);
                    ldNew.add(dmve);
                }
                if(other!=null&&!"".equals(other)){
                    Map<String, Object> map = new HashMap();
                    map.put(dimension, other);
                    map.put(name, other_num);
                    DimensionAndMetricValueExtractor dmve = new DimensionAndMetricValueExtractor(map);
                    ldNew.add(dmve);
                }
                ((TopNResultValue) ((Result) yielder.get()).getValue()).setValue(ldNew);
                al.add((Result) yielder.get());
                yielder = yielder.next(null);
            }
        } finally {
            try{
                yielder.close();
            }catch (Exception e){
             //   e.printStackTrace();
            }

        }
        Sequence s = Sequences.simple(al);
        long a=System.currentTimeMillis();
        long aa=a-t;
        return Yielders.each(s);
    }
    @Override
    public Yielder groupByDataHandle(Yielder yielder,HashMap resultModel){
        long t=System.currentTimeMillis();
        ResultChange resultChange = new ResultChange();
        Yielder y =resultChange.GroupByResultToTopNResult(yielder);
        long a=System.currentTimeMillis();
        LOG.info("结果集转换耗时======"+(a-t));
        return topNDataHandle(y,resultModel);
    }
}
