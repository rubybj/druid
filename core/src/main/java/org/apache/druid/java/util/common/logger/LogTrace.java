package org.apache.druid.java.util.common.logger;

import org.apache.druid.java.util.emitter.EmittingLogger;

import java.util.HashMap;
import java.util.Map;

public class LogTrace {
    static Map<String,Long> logMap= new HashMap<String,Long>();
    protected static final EmittingLogger Log = new EmittingLogger(LogTrace.class);
    static LogTrace logTrace=null;
    public static LogTrace getInstance(){
        if(logTrace==null){
            synchronized(LogTrace.class){
                if(logTrace==null) {
                    logTrace = new LogTrace();
                }
            }
        }
        return logTrace;
    }
    public void initTheardLog(){
        String name=Thread.currentThread().getName();
        logMap.put(name,System.currentTimeMillis());
    }
    public void writeCosttime(Class classes,String str){
        if(logMap.get(Thread.currentThread().getName())==null){
           initTheardLog();
        }

        Log.info("Calss"+classes.getName()+str+"cost:"+(System.currentTimeMillis()-logMap.get(Thread.currentThread().getName())));
    }





}
