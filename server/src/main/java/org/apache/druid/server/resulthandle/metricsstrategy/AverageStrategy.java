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
import org.apache.druid.server.resulthandle.DruidResultSecondDevHandler;
import org.apache.druid.server.resulthandle.DruidSecondDevConstant;
import org.apache.druid.server.resulthandle.tool.ResultChange;

import java.util.Iterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class AverageStrategy implements IMetricsStrategy {

    protected static final EmittingLogger LOG = new EmittingLogger(DruidResultSecondDevHandler.class);

    @Override
    public Yielder topNDataHandle(Yielder yielder, HashMap resultModel) {
        String name = "value";
        String groupKeys="";
        if (resultModel.containsKey(DruidSecondDevConstant.NAME)) {
            name = resultModel.get(DruidSecondDevConstant.NAME).toString();
        }
        if (resultModel.containsKey(DruidSecondDevConstant.GROUPKEYS)) {
            groupKeys = resultModel.get(DruidSecondDevConstant.GROUPKEYS).toString();
        }
        String metric = resultModel.get(DruidSecondDevConstant.METRIC).toString();
        ArrayList<Result> al = new ArrayList<Result>();
        try {
            while (!yielder.isDone()) {
                double metricSum = 0.0;
                double metricResult = 0.0;
                List<DimensionAndMetricValueExtractor> ldNew = new ArrayList<>();//用来存储新的结果值
                List<DimensionAndMetricValueExtractor> dnmves = ((TopNResultValue) ((Result) yielder.get()).getValue()).getValue();
                if (!resultModel.containsKey(DruidSecondDevConstant.METRIC)) {
                    return yielder;
                }
                int size = dnmves.size();
                //最后计算一次结果
                Map<String, Object> resultMap = new HashMap<>();
                if(groupKeys!=null&&!groupKeys.equals("")){
                    Map dataMap=installData(dnmves,groupKeys.split(","), metric);
                    installGroupMap(dataMap, groupKeys.split(","), 0);
                    resultMap=dataMap;
                }else {
                    //用来存储新的结果 这里结果样式要确定一下  是与之前的结果合并返回类似elasticsearch 那种的还是单独返回
                    for (DimensionAndMetricValueExtractor dnmve : dnmves) {
                        metricSum += dnmve.getDoubleMetric(metric);
                    }
                    metricResult = metricSum / size;
                    if(size==0){
                        metricResult = 0.0;
                    }
                    resultMap.put(name, metricResult);
                }

                //重新赋值
                DimensionAndMetricValueExtractor dmve = new DimensionAndMetricValueExtractor(resultMap);
                ldNew.add(dmve);
                ((TopNResultValue) ((Result) yielder.get()).getValue()).setValue(ldNew);
                al.add((Result) yielder.get());
                yielder = yielder.next(null);
            }
        } finally {
            try {
                yielder.close();
            } catch (IOException e) {
                LOG.error("对druid的二次处理报错,yielder.close();", e);
            }
        }

        Sequence s = Sequences.simple(al);
        Yielder y_result = Yielders.each(s);

        return y_result;
    }

    public  void installGroupMap(Map map,String[] groupKeys,int i){
        Iterator entries = map.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry entry = (Map.Entry) entries.next();
            Object key = entry.getKey();
            Map value = (Map)entry.getValue();
            if(i<groupKeys.length-2){
                installGroupMap(value,groupKeys,i+1);
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

    public  Map installData(List<DimensionAndMetricValueExtractor> dnmves,String[] groupKeys,String metric){
        Map datamap=new HashMap();
        for (DimensionAndMetricValueExtractor dnmve : dnmves) {
            Object metricValue=dnmve.getDoubleMetric(metric);
            installData0(datamap,dnmve,groupKeys,metricValue,0);
        }
        return datamap;
    }

    public  void installData0(Map map,DimensionAndMetricValueExtractor v,String[] groupKeys,Object metricValue,int i){
        String mapkey=(String)v.getMetric(groupKeys[i]);
        if(i==(groupKeys.length-1)){
            map.put(mapkey,metricValue);
        }else {
            i++;
            if(map.containsKey(mapkey)){
                installData0((Map)map.get(mapkey),v,groupKeys,metricValue,i);
            }else {
                Map map0=new HashMap();
                map.put(mapkey,map0);
                installData0(map0,v,groupKeys,metricValue,i);
            }
        }
    }

    @Override
    public Yielder groupByDataHandle(Yielder yielder, HashMap resultModel) {
        long t=System.currentTimeMillis();
        ResultChange resultChange = new ResultChange();
        Yielder y =resultChange.GroupByResultToTopNResult(yielder);
        LOG.info("结果集转换耗时======"+(System.currentTimeMillis()-t));
        return topNDataHandle(y,resultModel);
    }

    public static void main(String[] args){
        List<Map> list=new ArrayList();
        Map map=new HashMap();
        map.put("T","t0000");
        map.put("U","U1111");
        map.put("C","C0000");
        map.put("A","A0000");
        map.put("TUCA",1.0);
        list.add(map);

        Map map1=new HashMap();
        map1.put("T","t0000");
        map1.put("U","U1111");
        map1.put("C","C1111");
        map1.put("A","A2222");
        map1.put("TUCA",2.0);
        list.add(map1);

        Map map2=new HashMap();
        map2.put("T","t0000");
        map2.put("U","U2222");
        map2.put("C","C2222");
        map2.put("A","A34444");
        map2.put("TUCA",3.0);
        list.add(map2);
        Map map3=new HashMap();
        map3.put("T","t3333");
        map3.put("U","U2222");
        map3.put("C","C2222");
        map3.put("A","A55555");
        map3.put("TUCA",4.0);
        list.add(map3);
        Map datamap=new HashMap();
        for (Map m  : list) {
            //installData0(datamap,m,"T,U,C,A".split(","),m.get("TUCA"),0);
        }
        //installGroupMap(datamap,"T,U,C,A".split(","),0);
  //      System.out.println(datamap);
    }
}
