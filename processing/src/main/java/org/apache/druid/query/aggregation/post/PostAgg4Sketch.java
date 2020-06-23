package org.apache.druid.query.aggregation.post;

import javafx.geometry.Pos;
import org.apache.druid.java.util.emitter.EmittingLogger;

import java.lang.reflect.Method;

public class PostAgg4Sketch {
    protected static final EmittingLogger log = new EmittingLogger(PostAgg4Sketch.class);
    private static  PostAgg4Sketch postAgg4Sketch=null;
    public static PostAgg4Sketch getInstance(){
        if(postAgg4Sketch==null){
            synchronized (PostAgg4Sketch.class){
                if(postAgg4Sketch==null){
                    postAgg4Sketch=new PostAgg4Sketch();
                }

            }
        }
        return  postAgg4Sketch;
    }

    public Object getValue(Object obj){
        switch(obj.getClass().getName()){
            case "org.apache.druid.query.aggregation.datasketches.theta.SketchHolder":
                return getValue("getEstimate",obj);
            default:
                return obj;
        }



    }
    public Double getValue(String MethedName,Object obj){
        Double value=new Double(0);
        try {

            Method method = obj.getClass().getMethod(MethedName);
         value=(Double) method.invoke(obj);

        }catch(Exception e){
            log.warn("postagg value is error",e);
        }
          return value ;
    }
}
