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
import org.apache.druid.server.resulthandle.tool.Quantile;
import org.apache.druid.server.resulthandle.tool.ResultChange;
import org.eclipse.jetty.util.StringUtil;

import java.text.DecimalFormat;
import java.util.*;


public class QuantileStrategy implements IMetricsStrategy{
    protected static final EmittingLogger LOG = new EmittingLogger(QuantileStrategy.class);
    @Override
    public Yielder topNDataHandle(Yielder yielder, HashMap resultModel) {
        String dimension=(String)resultModel.get("dimension");
        String name=(String)resultModel.get("name");
        String order=(String)resultModel.get("order");
        Double quantile=Double.valueOf((String)resultModel.get("quantile"));
        String quantileLoc=(String)resultModel.get("quantile_loc");
        ArrayList<Result> al = new ArrayList<Result>();
        try{
            while (!yielder.isDone()) {
                List<DimensionAndMetricValueExtractor> ld=((TopNResultValue)((Result)yielder.get()).getValue()).getValue();
                List<DimensionAndMetricValueExtractor> ldNew=new ArrayList<DimensionAndMetricValueExtractor>();
                List<Double> dimensionList= new ArrayList<Double>();
                for(DimensionAndMetricValueExtractor dm:ld){
                    Double dimension_value=null;
                    dimension_value=Double.valueOf(dm.getMetric(dimension).toString());
                    dimensionList.add(dimension_value);
                }
                if("desc".equals(order)||"DESC".equals(order)){
                    //Collections.reverse(dimensionList);
                    quantile=1-quantile;
                }else if("asc".equals(order)||"ASC".equals(order)){
                    //Collections.sort(dimensionList);
                }
                double count=0;
                if(dimensionList.size()==1){
                    count=dimensionList.get(0);
                }else if(dimensionList.size()>1){
                    if(StringUtil.isNotBlank(quantileLoc)){
                        count= Quantile.quantileDescExc(dimensionList,Double.valueOf(quantileLoc));
                    }else{
                        count=Quantile.quantile_exc(dimensionList,quantile);
                    }
                }
                //DecimalFormat df = new DecimalFormat("#.00");

                //count=Double.valueOf(df.format(count));
                count=Double.valueOf(String.format(Locale.ENGLISH,"%.2f", count));
                Map<String, Object> map = new HashMap();
                map.put(name,count);
                DimensionAndMetricValueExtractor dmve =new DimensionAndMetricValueExtractor(map);
                ldNew.add(dmve);
                ((TopNResultValue)((Result)yielder.get()).getValue()).setValue(ldNew);
                al.add((Result)yielder.get());
                yielder = yielder.next(null);
            }
        }finally {
            try {
                yielder.close();
            }catch (Exception e){
             //   e.printStackTrace();
            }

        }
        Sequence s= Sequences.simple(al);
        return Yielders.each(s);
    }
    @Override
    public Yielder groupByDataHandle(Yielder yielder,HashMap resultModel){
        long t=System.currentTimeMillis();
        ResultChange resultChange = new ResultChange();
        Yielder y =resultChange.GroupByResultToTopNResult(yielder);
        LOG.info("结果集转换耗时======"+(System.currentTimeMillis()-t));
        return topNDataHandle(y,resultModel);
    }
}
