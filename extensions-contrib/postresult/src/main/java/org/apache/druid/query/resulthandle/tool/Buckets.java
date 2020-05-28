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

public class Buckets {
    //创建桶(度量是固定长度的)
    public static int[] createBuckets(int size,int bucketMetric){
        if(bucketMetric>100){
            bucketMetric=100;
        }
        int[] buckets= new int[(size/bucketMetric)+1];
        for(int i=0;i<buckets.length;i++){
            buckets[i]=i*bucketMetric;
            if(i*bucketMetric>100){
                buckets[i]=100;
            }
        }
        return buckets;
    }

    /**
     * @author jcc
     * @version 创建时间：2017年11月14日 上午09:23:26
     * @Description 创建桶(度量可以是不固定长度的，默认度量格式为 "0,50,80,90,95,97,100" )
     * @return 返回创建好的桶
     * @param bucketMetric 桶的划分标准
     */
    public static int[] createBuckets(String bucketMetric){
        //读取桶的划分标准
        String[] buckets_s= bucketMetric.split(",");
        //根据划分标准，生成相应数量的桶
        int[] buckets=new int[buckets_s.length];
        //规定每一个桶的取值范围
        for(int i=0;i<buckets_s.length;i++){
            buckets[i] = Integer.valueOf(buckets_s[i]);
        }
        return buckets;
    }

    /**
     * @author jcc
     * @version 创建时间：2017年11月14日 上午10:03:02
     * @Description 将数据放入桶中
     * @return 返回每个桶中对应的统计数据
     * @param buckets 创建的桶
     * @param data 需要放入桶中的数据
     * @param buckets_count  每个桶所对应的统计数据
     * @param metric_num  需要统计的标尺
     */
    public static int[] putDataToBuckets(int[] buckets,Double data,int[] buckets_count,int metric_num){
        //通过二分查找法，快速定位传入数据属于哪个桶
        int index=biSearch(buckets,data);
        //在对应的桶中，对对应的标尺进行统计
        buckets_count[index]=buckets_count[index]+metric_num;
        return buckets_count;
    }

    /*
        通过二分查找的方式快速定位传入数据属于哪个桶
     */
    public static int biSearch(int []array,Double a){
        int lo=0;
        int hi=array.length-1;
        int mid;
        while(lo<=hi){
            mid=(lo+hi)/2;
            if(array[mid]==a){
                return mid;
            }else if((array[mid]-a)*(array[mid+1]-a)<0){
                return mid;
            }else if(array[mid]<a){
                lo=mid+1;
            }else{
                hi=mid-1;
            }
        }
        return -1;
    }

    /**
     * 二分查找递归实现。
     * @param srcArray  有序数组
     * @param start 数组低地址下标
     * @param end   数组高地址下标
     * @param key  查找元素
     * @return 查找元素不存在返回-1
     */
    public static int binSearch(int[] srcArray, int start, int end, int key) {
        int mid = (end - start) / 2 + start;
        if (srcArray[mid] == key) {
            return mid;
        }
        if (start >= end) {
            return -1;
        } else if (key > srcArray[mid]) {
            return binSearch(srcArray, mid + 1, end, key);
        } else if (key < srcArray[mid]) {
            return binSearch(srcArray, start, mid - 1, key);
        }
        return -1;
    }


}
