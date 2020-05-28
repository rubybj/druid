/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.data.input;

import org.apache.commons.io.IOUtils;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
@PublicApi
public class MapBasedInputRow extends MapBasedRow implements InputRow
{
  private final List<String> dimensions;
  private static final Logger log = new Logger(MapBasedInputRow.class);
  private static Map<String,String> map = new HashMap<String,String>();


  public MapBasedInputRow(
      long timestamp,
      List<String> dimensions,
      Map<String, Object> event
  )
  {
    super(timestamp, event);
    if (dimensions.contains("jinshubao")){
      //添加自定义字段
      dealField();
    }
    this.dimensions = dimensions;
  }

  public MapBasedInputRow(
      DateTime timestamp,
      List<String> dimensions,
      Map<String, Object> event
  )
  {
    super(timestamp, event);
    if (dimensions.contains("jinshubao")){
      //添加自定义字段
      dealField();
    }
    this.dimensions = dimensions;
  }

  @Override
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @Override
  public String toString()
  {
    return "MapBasedInputRow{" +
           "timestamp=" + DateTimes.utc(getTimestampFromEpoch()) +
           ", event=" + getEvent() +
           ", dimensions=" + dimensions +
           '}';
  }
  private void initMap(){
    if (map.isEmpty()){
      InputStream ips = MapBasedInputRow.class.getClassLoader().getResourceAsStream("data.txt");
      List<String> lines = null;
      try {
        lines = IOUtils.readLines(ips);
      } catch (IOException e) {
        log.error("读取文件出错:"+e.getMessage());
      }
      for (String line : lines){
        String[] ls = line.split(",");
        map.put(ls[0],ls[1]);
      }
    }
  }

  /**
   * 修改源码在数据相互导入是对字段可以进行处理
   */
  private void dealField() {
    initMap();
    Map<String, Object> event = getEvent();
    browserType(event);
    screenSize(event);
    osType(event);
    usercode(event);
    modulecode(event);
    carrier(event);
    country(event);
    city(event);
    province(event);
    //有报账 ybz。 废弃了，不要多有报账单独处理，查询是通过appid来处理
//        ybz(event);
    //unique_tid 处理
    uniqueTid(event);
    terminalType(event);
    setEvent(event);
  }

  private void terminalType(Map<String, Object> event) {
    String terminalType = (String) event.get("terminal_type");
    if (terminalType != null){
      if (terminalType.contains("Windows")|| terminalType.contains("PC")){
        event.put("terminal_type","pc");
      }else if ("pad".equals(terminalType) || terminalType.contains("Android")
              || terminalType.contains("iOS")
              || terminalType.contains("iPhone") || terminalType.contains("Mac")
         ){
        event.put("terminal_type","mobile");
      }
    }
  }

  private void ybz(Map<String, Object> event){
    String tid = (String) event.get("tid");
    if (tid != null && "kHIrbaoQpK3288893848".equals(tid)){
      event.put("app_type","ybz");
    }
  }

  private void uniqueTid(Map<String, Object> event){
    if (event.containsKey("unique_tid")){
      return;
    }
    String tenantFullname = (String) event.get("tenant_fullname");
    if (tenantFullname != null){
      event.put("unique_tid",tenantFullname);
    }else {
      String tid = (String) event.get("tid");
      if (tid != null){
        event.put("unique_tid",tid);
      }
    }
  }

  private void country(Map<String, Object> event){
    String country = (String) event.get("country");
    if (country != null && "XX".equals(country)){
      event.put("country","未知");
    }
    if ("������".equals(country)){
      String ip = (String) event.get("ip");
      if ("113.204.210.10".equals(ip)){
        event.put("country","中国");
        event.put("province","重庆");
        event.put("city","重庆");
      }else if ("116.236.125.130".equals(ip)){
        event.put("country","中国");
        event.put("province","上海");
        event.put("city","上海");
      }else if ("220.173.103.59".equals(ip)){
        event.put("country","中国");
        event.put("province","广西");
        event.put("city","柳州");
      }else if ("58.249.59.140".equals(ip)){
        event.put("country","中国");
        event.put("province","广东");
        event.put("city","广州");
      }else if ("171.221.205.26".equals(ip)){
        event.put("country","中国");
        event.put("province","四川");
        event.put("city","成都");
      }else if ("122.5.31.114".equals(ip)){
        event.put("country","中国");
        event.put("province","山东");
        event.put("city","烟台");
      }else if ("58.20.232.202".equals(ip)){
        event.put("country","中国");
        event.put("province","湖南");
        event.put("city","湘潭");
      }else if ("220.249.1.214".equals(ip)){
        event.put("country","中国");
        event.put("province","北京");
        event.put("city","北京");
      }
    }
  }

  private void province(Map<String, Object> event){
    String province = (String) event.get("province");
    if (province != null && "XX".equals(province)){
      event.put("province","未知");
    }
    if ("东京都".equals(province)){
      String country = (String) event.get("country");
      if (country != null && "中国".equals(country)){
        event.put("country","日本");
      }
    }
  }

  private void city(Map<String, Object> event){
    String city = (String) event.get("city");
    if (city != null && "XX".equals(city)){
      event.put("city","未知");
    }
  }

  private void carrier(Map<String, Object> event){
    String carrier = (String) event.get("carrier");
    if (carrier != null && carrier.contains("/")){
      event.put("carrier","未知");
    }
//        if (carrier != null){
//            if ("未知".equals(carrier) || carrier.contains("/")){
//                event.put("carrier","内网");
//            }
//        }else {
//            event.put("carrier","内网");
//        }
  }

  private void browserType(Map<String, Object> event){
    String browserType = (String) event.get("browser_type");
    if (browserType != null && !"".equals(browserType)){
      if ("Internet".equals(browserType) || "IE".equals(browserType)){
        event.put("browser_type","Internet Explorer");
      }
    }
    String bv = (String) event.get("bv");
    if (bv != null && !"".equals(bv)){
      if (bv.contains("IE")){
        event.put("bv",bv.replace("IE","Internet Explorer"));
      }else if (bv.toLowerCase().contains("internet") && !bv.toLowerCase().contains("explorer")){
        event.put("bv",bv.replace("Internet","Internet Explorer"));
      }
    }

  }

  private void screenSize(Map<String, Object> event){
    String screenSize = (String) event.get("screen_size");
    if (screenSize != null && !"".equals(screenSize)){
      if (screenSize.contains("*")){
        String as = screenSize.replaceAll(" ","");
        event.put("screen_size",as);
      }else if (screenSize.contains("×")){
        String as = screenSize.replaceAll("×","*");
        event.put("screen_size",as);
      }else if(screenSize.contains("���")){
        String as = screenSize.replaceAll("���","*");
        event.put("screen_size",as);
      }
//            if (screenSize.contains("*")){
//                String as = screenSize.replaceAll("\\*","×").replaceAll(" ","");
//                event.put("screen_size",as);
//            }
    }
  }

  private void osType(Map<String, Object> event){
    String osType = (String) event.get("os_type");
    if (osType == null || "".equals(osType)){
      event.remove("os_type");
      return;
    }
    if (osType.contains("Windows 7")){
      event.put("os_type","Windows 7");
    }else if(osType.contains("Windows XP")){
      event.put("os_type","Windows XP");
    }else if(osType.contains("Windows 8.1")){
      event.put("os_type","Windows 8.1");
    }else if(osType.contains("Windows 8")){
      event.put("os_type","Windows 8");
    }else if(osType.contains("Windows 10")){
      event.put("os_type","Windows 10");
    }else if(osType.contains("Windows Server 2008 R2")){
      event.put("os_type","Windows Server 2008 R2");
    }else if(osType.contains("Windows Server 2008")){
      event.put("os_type","Windows Server 2008");
    }else if(osType.contains("Windows Server 2012 R2")){
      event.put("os_type","Windows Server 2012 R2");
    }else if(osType.contains("Windows Server 2012")){
      event.put("os_type","Windows Server 2012");
    }else if(osType.contains("ios") || osType.contains("iOS")){
      if (osType.contains("iOS 9") || osType.contains("ios 9")){
        event.put("os_type","iOS 9");
      }else if (osType.contains("iOS 10") || osType.contains("ios 10")){
        event.put("os_type","iOS 10");
      }else if (osType.contains("iOS 11") || osType.contains("ios 11")){
        event.put("os_type","iOS 11");
      }else if (osType.contains("iOS 12") || osType.contains("ios 12")){
        event.put("os_type","iOS 12");
      }else{
        event.put("os_type",osType.toUpperCase().split("\\.")[0]);
      }
    }else if(osType.contains("android")){
      if (osType.contains("android 5")){
        event.put("os_type","Android 5.x");
      }else if (osType.contains("android 2")){
        event.put("os_type","Android 2.x");
      }else if (osType.contains("android 4")){
        event.put("os_type","Android 4.x");
      }else if (osType.contains("android 6")){
        event.put("os_type","Android 6.x");
      }else if (osType.contains("android 7")){
        event.put("os_type","Android 7.x");
      }else if (osType.contains("android 8")){
        event.put("os_type","Android 8.x");
      }else if (osType.contains("android 9")){
        event.put("os_type","Android 9.x");
      }else{
        event.put("os_type",osType.replaceAll("android","Android").split("\\.")[0]);
      }
    }else if(osType.contains("Android")){
      if (osType.contains("Android 5")){
        event.put("os_type","Android 5.x");
      }else if (osType.contains("Android 2")){
        event.put("os_type","Android 2.x");
      }else if (osType.contains("Android 4")){
        event.put("os_type","Android 4.x");
      }else if (osType.contains("Android 6")){
        event.put("os_type","Android 6.x");
      }else if (osType.contains("Android 7")){
        event.put("os_type","Android 7.x");
      }else if (osType.contains("Android 8")){
        event.put("os_type","Android 8.x");
      }else if (osType.contains("Android 9")){
        event.put("os_type","Android 9.x");
      }else{
        event.put("os_type",osType.split("\\.")[0]);
      }
    }else if(osType.contains("Windows") && osType.contains("Server") && osType.contains("2003") && osType.contains("R2")){
      event.put("os_type","Windows Server 2003 R2");
    }else if(osType.contains("Windows") && osType.contains("Server") && osType.contains("2003")){
      event.put("os_type","Windows Server 2003");
    }else if(osType.contains("ANDROID6_TABLET") || osType.contains("ANDROID6")){
      event.put("os_type","Android 6.x");
    }else if(osType.contains("iOS8_4_IPAD")){
      event.put("os_type","IOS 8");
    }else if(osType.contains("Windows Web Server 2008")){
      event.put("os_type","Windows Server 2008");
    }else if(osType.contains("Windows") && osType.contains("Embedded")){
      event.put("os_type","Windows Embedded");
    }else if(osType.contains("Microsoft Windows 10 Enterprise N 2016 LTSB")){
      event.put("os_type","Windows 10");
    }else if(osType.contains("Windows") && osType.contains("Server") && osType.contains("2008")){
      event.put("os_type","Windows Server 2008");
    }else if(osType.contains("WINDOWS_81")){
      event.put("os_type","Windows 8.1");
    }else if(osType.contains("ANDROID5")){
      event.put("os_type","Android 5.x");
    }else if(osType.contains("Windows Server 2016 R2")){
      event.put("os_type","Windows Server 2016 R2");
    }else if(osType.contains("Windows Server 2016")){
      event.put("os_type","Windows Server 2016");
    }else if(osType.contains("Windows Server 2019 R2")){
      event.put("os_type","Windows Server 2019 R2");
    }else if(osType.contains("Windows Server 2019")){
      event.put("os_type","Windows Server 2019");
    }else if(osType.contains("Windows") || osType.contains("Vista")){
      event.put("os_type","Windows Vista");
    }else if(osType.contains("Windows") || osType.contains("XP")){
      event.put("os_type","Windows XP");
    }else if(!(osType.trim().length() > 0)){
      event.remove("os_type");
    }
  }

  /**
   * 为了区分不同租户下面的相同user_code，后缀统一添加tid的后四位
   * @param event
   */
  private void usercode(Map<String, Object> event){
    if (!event.containsKey("unique_user_code")){
      String userCode = (String) event.get("user_code");
      if (userCode != null){
        if (userCode.startsWith("unknown-")){
          String userName = (String) event.get("user_name");
          if (userName != null){
            userCode = userName;
          }
        }
        String tid = (String) event.get("tid");
        if (tid != null){
          String suffix = tid;
          if (tid.length() >  10){
            suffix = tid.substring(0,10);
          }
          event.put("unique_user_code", userCode + "-" + suffix);
        }
      }
    }
  }

  private void modulecode(Map<String, Object> event) {
    String domainname = (String) event.get("domainname");
    if (!event.containsKey("modulecode") || !event.containsKey("modulename")|| !event.containsKey("domaincode")
            || !event.containsKey("domainname") || domainname == null){
      String nodeCode = (String) event.get("node_code");
      if (nodeCode != null && !"".equals(nodeCode) && nodeCode.length() > 2) {
        if (nodeCode.length() >= 4){
          String modulecode4 = nodeCode.substring(0, 4);
          if (map.containsKey(modulecode4)){
            addModule(event,modulecode4,nodeCode,2);
          }else {
            addModuleIndex3(event,nodeCode);
          }
        }else if (nodeCode.length()>=3){
          addModuleIndex3(event,nodeCode);
        }
      }
    }
  }

  private void addModuleIndex3(Map<String, Object> event,String nodeCode){
    String modulecode = nodeCode.substring(0, 3);
    if (map.containsKey(modulecode)){
      addModule(event,modulecode,nodeCode,1);
    }
  }

  private void addModule(Map<String, Object> event,String modulecode,String nodeCode,int index){
    event.put("modulecode", modulecode);
    event.put("modulename",map.get(modulecode));
    String domaincode = nodeCode.substring(0,index);
    event.put("domaincode",domaincode);
    event.put("domainname",map.get(domaincode));
  }




}
