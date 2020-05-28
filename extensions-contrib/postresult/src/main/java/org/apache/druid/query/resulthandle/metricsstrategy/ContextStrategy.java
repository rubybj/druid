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



import org.apache.druid.query.resulthandle.tool.ResultConst;

import java.util.Map;

public class ContextStrategy {
    public IMetricsStrategy metricsStrategy;

    public void setMetricsStrategy(IMetricsStrategy metricsStrategy) {
        this.metricsStrategy = metricsStrategy;
    }

    public IMetricsStrategy getMetricsStrategy() {
        return metricsStrategy;
    }

    public void getMetricsResult(String type,Map<String, Object> values){
        switch (type){
            case ResultConst.TOPN:

                metricsStrategy.topNDataHandle(values);
                break;
            case ResultConst.GROUPBY:
                 metricsStrategy.groupByDataHandle(values);
                 break;
            default:return ;
        }
    }
}
