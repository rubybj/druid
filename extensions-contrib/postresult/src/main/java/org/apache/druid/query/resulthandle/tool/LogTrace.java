package org.apache.druid.query.resulthandle.tool;

import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.resulthandle.DruidResultSecondDevHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;

public class LogTrace {
    static Map<String,Long> logMap=new HashMap<String,Long>();
    protected static final EmittingLogger Log = new EmittingLogger(LogTrace.class);
    static LogTrace logTrace=null;
    public LogTrace getInstance(){
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
        if((System.currentTimeMillis()-logMap.get(Thread.currentThread().getName()))>10000){
            Log.info("Class"+classes.getName()+str+"cost:"+(System.currentTimeMillis()-logMap.get(Thread.currentThread().getName())));
        }

    }





}
