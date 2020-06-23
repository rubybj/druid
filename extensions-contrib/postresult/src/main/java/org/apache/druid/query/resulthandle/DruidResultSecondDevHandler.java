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
package org.apache.druid.query.resulthandle;


import com.google.common.base.Strings;
import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.Query;
import org.apache.druid.query.Result;
import org.apache.druid.query.resulthandle.metricsfactory.AverageFactory;
import org.apache.druid.query.resulthandle.metricsfactory.BaseMetricsFactory;
import org.apache.druid.query.resulthandle.metricsfactory.DistributionFactory;
import org.apache.druid.query.resulthandle.metricsfactory.QuantileFactory;
import org.apache.druid.query.resulthandle.metricsstrategy.ContextStrategy;
import org.apache.druid.query.resulthandle.metricsstrategy.DistributionStrategy;
import org.apache.druid.query.resulthandle.metricsstrategy.IMetricsStrategy;
import org.apache.druid.query.resulthandle.tool.Buckets;
import org.apache.druid.query.resulthandle.tool.Quantile;
import org.apache.druid.query.resulthandle.tool.ResultConst;
import org.apache.druid.query.topn.DimensionAndMetricValueExtractor;
import org.apache.druid.query.topn.TopNResultValue;
import org.eclipse.jetty.util.StringUtil;

import java.lang.reflect.Array;
import java.util.*;

/*
 * Created by wumr3 on 17/12/7.
 * 这个类作用是对druid的结果集进行二次开发
 */
public class DruidResultSecondDevHandler {
    /**
     * @param yielder 第一次druid查询返回的记过
     * @param type 查询类型
     * @param resultType 返回结果类型
     * @param resultModel 结果二次处理的请求参数
     * @return
     */

    //protected static final EmittingLogger LOG = new EmittingLogger(DistributionStrategy.class);
    protected static final EmittingLogger LOG = new EmittingLogger(DruidResultSecondDevHandler.class);
    static final ThreadLocal<Map> sThreadLocal = new ThreadLocal<Map>();
    static DruidResultSecondDevHandler druidResultSecondDevHandler = new DruidResultSecondDevHandler();

    public ThreadLocal<Map> getThreadLocal() {
        return sThreadLocal;
    }

    public static DruidResultSecondDevHandler getInstance() {
        return druidResultSecondDevHandler;

    }

    public void secondDevResultHandler(Map<String, Object> values, Query query, String resultType, HashMap resultModel) {
        BaseMetricsFactory metricsFactory;
        String type = query.getType();

        if (resultModel == null || resultType == null) {
            return ;
        }

        Map resultMap= DruidResultSecondDevHandler.getInstance().getThreadLocal().get();


        switch (resultType) {
            case ResultConst.AVGBUCKET:
                if(resultMap==null) {
                    this.initAvgBuckets(resultType, resultModel);
                }
                metricsFactory = new AverageFactory();
                break;
            case ResultConst.DISTRIBUTION:
                if(resultMap==null){
                this.initDistributionBuckets(resultType,resultModel);
                }
                metricsFactory = new DistributionFactory();
                break;
            case ResultConst.QUANTILE:
                if(resultMap==null){
                    this.initQuantile(resultType,resultModel);
                }
                metricsFactory = new QuantileFactory();
                break;
            default:
                return ;
        }
        IMetricsStrategy metricsStrategy = metricsFactory.createMetrics();
        ContextStrategy contextStrategy = new ContextStrategy();
        contextStrategy.setMetricsStrategy(metricsStrategy);
        contextStrategy.getMetricsResult(type, values);
    }

    public List getResult() {
        if(DruidResultSecondDevHandler.getInstance().getThreadLocal().get()==null){
            return null;
        }

       String resultType= (String)DruidResultSecondDevHandler.getInstance().getThreadLocal().get().get(ResultConst.RESULTTYPE);

       switch (resultType) {
            case ResultConst.AVGBUCKET:
                return getAVG();

            case ResultConst.DISTRIBUTION:

             return getDistribution();


            case ResultConst.QUANTILE:
                //metricsFactory = new QuantileFactory();
                return getQuantile();

            default:
                return null;
        }
    }
    private List getDistribution(){

        Map resultMap=  DruidResultSecondDevHandler.getInstance().getThreadLocal().get();
        int[] buckets_count=(int[])resultMap.get(ResultConst.BUCKETSCOUNT);
        int[] buckets=(int[])resultMap.get(ResultConst.BUCKETS);
        String[] bucketDes = (String[])resultMap.get(ResultConst.BUCKETSDES);
        String metric=(String)resultMap.get(ResultConst.METRIC);
        String name=(String)resultMap.get(ResultConst.NAME);
        String dimension=(String)resultMap.get(ResultConst.DIMENSION);
        String other = (String)resultMap.get(ResultConst.OTHERNAME);
        int other_num = (int)resultMap.get(ResultConst.OTHERNUM);

        List<Map> ldNew = new ArrayList<Map>();
        for (int i = 0; i < buckets_count.length - 1; i++) {
            Map<String, Object> map = new HashMap();
            if (bucketDes != null) {
                map.put(dimension, bucketDes[i]);
            } else {
                map.put(dimension, buckets[i] + "-" + buckets[i + 1]);
            }
            map.put(name, buckets_count[i]);
           // DimensionAndMetricValueExtractor dmve = new DimensionAndMetricValueExtractor(map);
            ldNew.add(map);
        }
        if(other!=null&&!"".equals(other)){
            Map<String, Object> map = new HashMap();
            map.put(dimension, other);
            map.put(name, other_num);
            //DimensionAndMetricValueExtractor dmve = new DimensionAndMetricValueExtractor(map);
            ldNew.add(map);
        }

       // Sequence s = Sequences.simple(ldNew);
        return ldNew;

    }
    private List getAVG(){
        //加入groupkey的未见规范，估暂不返回。

        Map paramMap=  DruidResultSecondDevHandler.getInstance().getThreadLocal().get();
        String groupKeys = null;


        List resultList=null;
        if(paramMap.containsKey(ResultConst.RESULT)){
            HashMap result=(HashMap) paramMap.get(ResultConst.RESULT);
            resultList=new ArrayList();
            for(Object key:result.keySet()){
                double[] comp=(double[])result.get(key);
                if(comp[1]==0.0){
                    result.put(key.toString(),0.0);
                }else {
                    double value=comp[0]/comp[1];
                    result.put(key.toString(),value);
                }

            }

            resultList.add(paramMap.get(ResultConst.RESULT));
        }else{
            return null;
        }




        // Sequence s = Sequences.simple(ldNew);
        return resultList;

    }

    /**
     * 之前算法未做调整，只做适配。
     * @return
     */
    private List getQuantile(){
        Map resultModel=  DruidResultSecondDevHandler.getInstance().getThreadLocal().get();
        String dimension=(String)resultModel.get(ResultConst.DIMENSION);
        String name=(String)resultModel.get(ResultConst.NAME);
        String order=(String)resultModel.get(ResultConst.ORDER);
        Double quantile=Double.valueOf(resultModel.get(ResultConst.QUANTILE).toString());
        String quantileLoc=(String)resultModel.get(ResultConst.QUANTILELOC);
        ArrayList results = new ArrayList();
        List<Map> valueList=(ArrayList<Map>)resultModel.get(ResultConst.RESULT);
        try {

            List<Double> dimensionList = new ArrayList<Double>();
            for (Map dm : valueList) {
                Double dimension_value = null;
                dimension_value = Double.valueOf(dm.get(dimension).toString());
                dimensionList.add(dimension_value);
            }
            if ("desc".equals(order) || "DESC".equals(order)) {
                //Collections.reverse(dimensionList);
                quantile = 1 - quantile;
            } else if ("asc".equals(order) || "ASC".equals(order)) {
                //Collections.sort(dimensionList);
            }
            double count = 0;
            if (dimensionList.size() == 1) {
                count = dimensionList.get(0);
            } else if (dimensionList.size() > 1) {
                if (StringUtil.isNotBlank(quantileLoc)) {
                    count = Quantile.quantileDescExc(dimensionList, Double.valueOf(quantileLoc));
                } else {
                    count = Quantile.quantile_exc(dimensionList, quantile);
                }
            }
            //DecimalFormat df = new DecimalFormat("#.00");

            //count=Double.valueOf(df.format(count));
            count = Double.valueOf(String.format(Locale.ENGLISH, "%.2f", count));
            Map<String, Object> resultmap = new HashMap();
            resultmap.put(name, count);
            results.add(resultmap);
        }catch(Exception e){
            LOG.warn("分位值算法出现错误",e);
        }
                return results;


    }
    private void initDistributionBuckets(String resultType,Map<String,Object>  resultModel){
        String other = (String) resultModel.get("other");
        String name = (String) resultModel.get("name");
        String demension = (String) resultModel.get("dimension");
        String metric = (String) resultModel.get("metric");
        Integer bucketMetricFix = (Integer) resultModel.get("bucketMetricFix");
        String bucketMetricFree = (String) resultModel.get("bucketMetricFree");
        String bucketMetricDescrip = (String) resultModel.get("bucketMetricDescrip");
        int size = 100;
        int bucketMetric = 100;
        ArrayList<Result> al = new ArrayList<Result>();

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
        HashMap<String,Object> resultMap=new HashMap<String,Object>();
        resultMap.put(ResultConst.BUCKETS,buckets);
        resultMap.put(ResultConst.METRIC,metric);
        resultMap.put(ResultConst.BUCKETSDES,bucketDes);
        resultMap.put(ResultConst.OTHERNUM,0);
        resultMap.put(ResultConst.NAME,name);
        resultMap.put(ResultConst.DIMENSION,demension);
        resultMap.put(ResultConst.OTHERNAME,other);
        resultMap.put(ResultConst.RESULTTYPE,resultType);
        resultMap.put(ResultConst.BUCKETSCOUNT,new int[buckets.length]);
        DruidResultSecondDevHandler.getInstance().getThreadLocal().set(resultMap);
    }
    private void initAvgBuckets(String resultType,Map<String,Object>  resultModel){
        String name = "value";
        String groupKeys="";
        String metric="";
        if(resultModel.containsKey(ResultConst.NAME)){
            name = resultModel.get(ResultConst.NAME).toString();
        }

        if(resultModel.containsKey(ResultConst.GROUPKEYS)) {
            groupKeys = resultModel.get(ResultConst.GROUPKEYS).toString();
        }
        if(resultModel.containsKey(ResultConst.METRIC)){
             metric= resultModel.get(ResultConst.METRIC).toString();
        }

        HashMap<String,Object> pramaMap=new HashMap<String,Object>();
        HashMap result=new HashMap();

        pramaMap.put(ResultConst.METRIC,metric);
        pramaMap.put(ResultConst.OTHERNUM,0);
        pramaMap.put(ResultConst.NAME,name);
        pramaMap.put(ResultConst.RESULTTYPE,resultType);
        pramaMap.put(ResultConst.RESULT,result);
        DruidResultSecondDevHandler.getInstance().getThreadLocal().set(pramaMap);
    }

    /**
     * 分位值应该是采用了某个估算算法，这里不做修改。
     * 只做结构的调整。
     */
    private void initQuantile(String resultType,Map<String,Object>  resultModel){
        String dimension=(String)resultModel.get("dimension");
        String name=(String)resultModel.get("name");
        String order ="asc";
        if(resultModel.containsKey(ResultConst.ORDER)) {
            order = (String) resultModel.get("order");
        }

        Double quantile=Double.valueOf((String)resultModel.get("quantile"));
        String quantileLoc=(String)resultModel.get("quantile_loc");
        List datas=new ArrayList();//看算法只有7个，存在本地缓存应该可以。
        HashMap<String,Object> pramaMap=new HashMap<String,Object>();
        pramaMap.put(ResultConst.NAME,name);
        pramaMap.put(ResultConst.RESULTTYPE,resultType);
        pramaMap.put(ResultConst.DIMENSION,dimension);
        pramaMap.put(ResultConst.ORDER,order);
        pramaMap.put(ResultConst.QUANTILE,quantile);
        pramaMap.put(ResultConst.QUANTILELOC,quantileLoc);
        pramaMap.put(ResultConst.RESULT,datas);
        DruidResultSecondDevHandler.getInstance().getThreadLocal().set(pramaMap);




    }
}

