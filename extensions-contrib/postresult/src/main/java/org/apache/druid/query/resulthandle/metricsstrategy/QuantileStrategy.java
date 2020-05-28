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
import org.eclipse.jetty.util.StringUtil;
import org.apache.druid.query.resulthandle.tool.Quantile;
import org.apache.druid.query.resulthandle.tool.ResultChange;

import java.util.*;


public class QuantileStrategy implements IMetricsStrategy {
    protected static final EmittingLogger LOG = new EmittingLogger(QuantileStrategy.class);
    @Override
    public void topNDataHandle(Map<String,Object> values) {

        List result = (ArrayList) DruidResultSecondDevHandler.getInstance().getThreadLocal().get().get(ResultConst.RESULT);
        result.add(values);
    }

    @Override
    public void groupByDataHandle(Map<String,Object> values){
       /**
        long t=System.currentTimeMillis();
        ResultChange resultChange = new ResultChange();
        Yielder y =resultChange.GroupByResultToTopNResult(yielder);
        LOG.info("结果集转换耗时======"+(System.currentTimeMillis()-t));
        return topNDataHandle(y,resultModel);
        **/
       return ;
    }
}
