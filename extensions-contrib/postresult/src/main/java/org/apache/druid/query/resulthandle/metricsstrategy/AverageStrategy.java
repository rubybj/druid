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


import it.unimi.dsi.fastutil.doubles.Double2BooleanArrayMap;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.resulthandle.tool.ResultConst;
import org.apache.druid.query.topn.DimensionAndMetricValueExtractor;
import org.apache.druid.query.resulthandle.DruidResultSecondDevHandler;
import org.apache.druid.query.resulthandle.tool.ResultChange;

import java.util.*;


public class AverageStrategy implements IMetricsStrategy {

    protected static final EmittingLogger LOG = new EmittingLogger(DruidResultSecondDevHandler.class);

    @Override
    public void topNDataHandle(Map<String, Object> values) {
        String name = "";
        String groupKeys="";
        Map resultModel=  DruidResultSecondDevHandler.getInstance().getThreadLocal().get();

        if (resultModel.containsKey(ResultConst.NAME)) {
            name = resultModel.get(ResultConst.NAME).toString();
        }
        if (resultModel.containsKey(ResultConst.GROUPKEYS)) {
            groupKeys = resultModel.get(ResultConst.GROUPKEYS).toString();
        }
        String metric = resultModel.get(ResultConst.METRIC).toString();
        try {

                if (!resultModel.containsKey(ResultConst.METRIC)) {
                    return ;
                }
                //最后计算一次结果
               Map<String, Object> resultMap=(HashMap)resultModel.get(ResultConst.RESULT);
                if(groupKeys!=null&&!groupKeys.equals("")){
                   installData(values,groupKeys.split(","), metric);
                    //installGroupMap(resultMap,values, groupKeys.split(","), 0);
                }else {
                    //用来存储新的结果 这里结果样式要确定一下  是与之前的结果合并返回类似elasticsearch 那种的还是单独返回
                    double[] metricSum=(double[]) resultMap.get(name);
                    if(metricSum==null){
                        metricSum=new double[]{0.0,0.0};
                    }
                    metricSum[0] += Double.valueOf(values.get(metric).toString());
                    metricSum[1]+=1.0;

                    resultMap.put(name, metricSum);
                }

                //重新赋值

            } catch (Exception e) {
                LOG.error("对druid的二次处理报错,yielder.close();", e);
            }


        return ;
    }

    public  void installGroupMap(Map resultMap,Map map,String[] groupKeys,int i){

        Iterator entries = map.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry entry = (Map.Entry) entries.next();
            Object key = entry.getKey();
            Map value = (Map)entry.getValue();
            if(i<groupKeys.length-2){
               // installGroupMap(value,groupKeys,i+1);
            }
            map.put(key,groupCount(value));
        }
    }




    public  Object groupCount(Map map){
        Iterator entries = map.entrySet().iterator();
        int size=map.size();
        double valueSum=0.0;
        while (entries.hasNext()) {
            Map.Entry entry = (Map.Entry) entries.next();
            Double value = (Double) entry.getValue();
            valueSum=valueSum+value;
        }
        return valueSum/size;
    }

    public  Map installData(Map values,String[] groupKeys,String metric){
        Map datamap=new HashMap();
            Object metricValue=Double.valueOf(values.get(metric).toString());
            installData0(datamap,values,groupKeys,metricValue,0);
        return datamap;
    }

    public  void installData0(Map map,Map values,String[] groupKeys,Object metricValue,int i){
        String mapkey=(String)values.get(groupKeys[i]);
        if(i==(groupKeys.length-1)){
            map.put(mapkey,metricValue);
        }else {
            i++;
            if(map.containsKey(mapkey)){
                installData0((Map)map.get(mapkey),values,groupKeys,metricValue,i);
            }else {
                Map map0=new HashMap();
                map.put(mapkey,map0);
                installData0(map0,values,groupKeys,metricValue,i);
            }
        }
    }

    @Override
    public void groupByDataHandle(Map<String,Object> values) {
       /*
        long t=System.currentTimeMillis();
        ResultChange resultChange = new ResultChange();
        Yielder y =resultChange.GroupByResultToTopNResult(yielder);
        LOG.info("结果集转换耗时======"+(System.currentTimeMillis()-t));
        return topNDataHandle(y,resultModel);
        */
       return ;
    }

}
