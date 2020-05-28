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
package org.apache.druid.query.resulthandle.tool;

import java.util.List;

public class Quantile {
    /*
    对于四分位数的确定，有不同的方法，另外一种方法基于N-1 基础。即
    Q1的位置=1+（n-1）x 0.25
    Q2的位置=1+（n-1）x 0.5
    Q3的位置=1+（n-1）x 0.75
     */
    public static double quantile_exc(List<Double> list, double quantile){
        if(list.size()==1){
            return list.get(0);
        }else{
            double index=1+(list.size()-1)*quantile;
            int low=(int)index;
            double mid=index-low;
            return list.get(low)*mid+(1-mid)*list.get(low-1);
        }

    }


    public static double quantileDescExc(List<Double> list,Double loc){
        if(list.size()==1){
            return list.get(0);
        }else{
            Double U=list.get(list.size()-2);
            Double L=list.get(list.size()-1);
            //return Arith.add(Arith.mul(Arith.sub(U,L),Arith.sub(loc,loc.intValue())),L);
            return (U-L)*(loc-loc.intValue())+L;
        }

    }


    /*
    基于N+1 基础。即
    Q1的位置= (n+1) × 0.25
    Q2的位置= (n+1) × 0.5
    Q3的位置= (n+1) × 0.75
     */
    public static void quantile_inc(List<Double> list,double quantile){

    }


}
