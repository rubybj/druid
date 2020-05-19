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



import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.server.resulthandle.DruidSecondDevConstant;

import java.util.HashMap;

public class ContextStrategy {
    public IMetricsStrategy metricsStrategy;

    public void setMetricsStrategy(IMetricsStrategy metricsStrategy) {
        this.metricsStrategy = metricsStrategy;
    }

    public IMetricsStrategy getMetricsStrategy() {
        return metricsStrategy;
    }

    public Yielder getMetricsResult(String type, Yielder yielder, HashMap resultModel){
        switch (type){
            case DruidSecondDevConstant.TOPN:
                return metricsStrategy.topNDataHandle(yielder,resultModel);
            case DruidSecondDevConstant.GROUPBY:
                return metricsStrategy.groupByDataHandle(yielder,resultModel);
            default:return yielder;
        }
    }
}
