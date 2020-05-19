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
package org.apache.druid.server.resulthandle;


import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.resulthandle.metricsfactory.AverageFactory;
import org.apache.druid.server.resulthandle.metricsfactory.BaseMetricsFactory;
import org.apache.druid.server.resulthandle.metricsfactory.DistributionFactory;
import org.apache.druid.server.resulthandle.metricsfactory.QuantileFactory;
import org.apache.druid.server.resulthandle.metricsstrategy.ContextStrategy;
import org.apache.druid.server.resulthandle.metricsstrategy.IMetricsStrategy;

import java.util.HashMap;
/*
 * Created by wumr3 on 17/12/7.
 * 这个类作用是对druid的结果集进行二次开发
 */
public class DruidResultSecondDevHandler {
    /**
     *
     * @param yielder 第一次druid查询返回的记过
     * @param type 查询类型
     * @param resultType 返回结果类型
     * @param resultModel 结果二次处理的请求参数
     * @return
     */


    protected static final EmittingLogger LOG = new EmittingLogger(DruidResultSecondDevHandler.class);
    public static Yielder secondDevResultHandler(Yielder yielder, String resultType, String type, HashMap resultModel) {
        BaseMetricsFactory metricsFactory;
        if(resultModel==null||resultType==null){
            return yielder;
        }
        switch (resultType){
            case DruidSecondDevConstant.AVGBUCKET:
                metricsFactory=new AverageFactory();
                break;
            case DruidSecondDevConstant.DISTRIBUTION:
                metricsFactory=new DistributionFactory();
                break;
            case DruidSecondDevConstant.QUANTILE:
                metricsFactory=new QuantileFactory();
                break;
            default :  return yielder;
        }
        IMetricsStrategy metricsStrategy=metricsFactory.createMetrics();
        ContextStrategy contextStrategy=new ContextStrategy();
        contextStrategy.setMetricsStrategy(metricsStrategy);
        return contextStrategy.getMetricsResult(type,yielder,resultModel);
    }
}
