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

package org.apache.druid.server.resulthandle.tool;


import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.query.Result;
import org.apache.druid.query.topn.DimensionAndMetricValueExtractor;
import org.apache.druid.query.topn.TopNResultValue;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ResultChange {
    /*
    改方法主要是为了将GroupBy返回的结果集转换为TopN返回的结果集类型 以便能适应以前对topN返回结果集的处理方法
    暂定方案，不是很确定对效率产生多大的影响
     */
    public Yielder GroupByResultToTopNResult(Yielder yielder){
        ArrayList<Result> al = new ArrayList<Result>();
        ArrayList<HashMap> hmlist = new ArrayList<HashMap>();
        try{
            List<DateTime> dtlist=new ArrayList<>();
            while (!yielder.isDone()) {
                HashMap<String, Object> dm=(HashMap<String, Object>)((MapBasedRow)yielder.get()).getEvent();
                DateTime dateTime=((MapBasedRow)yielder.get()).getTimestamp();
                if(dtlist.size()==0||!dtlist.contains(dateTime)){
                    dtlist.add(dateTime);
                    ArrayList<DimensionAndMetricValueExtractor>  ldNew = new ArrayList<DimensionAndMetricValueExtractor>();
                    ldNew.add(new DimensionAndMetricValueExtractor(dm));
                    HashMap hm=new HashMap();
                    hm.put("dateTime",dateTime);
                    hm.put("ldNew",ldNew);
                    hmlist.add(hm);
                }else {
                    for(HashMap hm:hmlist){
                        if(dateTime.equals(hm.get("dateTime"))){
                            ((ArrayList<DimensionAndMetricValueExtractor>)hm.get("ldNew")).add(new DimensionAndMetricValueExtractor(dm));
                        }
                    }
                }
                yielder = yielder.next(null);
            }
        }finally {
            try{
                yielder.close();
            }catch (Exception e){
               // e.printStackTrace();
            }

        }
        for(HashMap hm:hmlist){
            TopNResultValue topNResultValue=new TopNResultValue((ArrayList<DimensionAndMetricValueExtractor>) hm.get("ldNew"));
            Result result=new Result((DateTime)hm.get("dateTime"),topNResultValue);
            al.add(result);
        }
        Sequence s= Sequences.simple(al);
        return Yielders.each(s);
    }
}
