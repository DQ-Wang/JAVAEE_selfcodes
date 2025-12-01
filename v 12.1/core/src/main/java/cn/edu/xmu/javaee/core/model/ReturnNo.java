//School of Informatics Xiamen University, GPL-3.0 license
package cn.edu.xmu.javaee.core.model;

import java.util.HashMap;
import java.util.Map;

/**
 * 返回的错误码
 * @author Ming Qiu
 */
public enum ReturnNo {
    /***************************************************
     *    系统级返回码
     **************************************************/

    //状态码 200
    OK(0,"OK"),
    CREATED(1, "CREATED"),
    STATENOTALLOW(7,"STATENOTALLOW"),
    RESOURCE_FALSIFY(11, "RESOURCE_FALSIFY"),

    //状态码 404
    RESOURCE_ID_NOTEXIST(4,"RESOURCE_ID_NOTEXIST"),

    //状态码 500
    INTERNAL_SERVER_ERR(2,"INTERNAL_SERVER_ERR"),
    APPLICATION_PARAM_ERR(20, "APPLICATION_PARAM_ERR"),

    //所有需要登录才能访问的API都可能会返回以下错误
    //状态码 400
    FIELD_NOTVALID(3,"FIELD_NOTVALID"),
    IMG_FORMAT_ERROR(8,"IMG_FORMAT_ERROR"),
    IMG_SIZE_EXCEED(9,"IMG_SIZE_EXCEED"),
    PARAMETER_MISSED(10, "PARAMETER_MISSED"),
    INCONSISTENT_DATA(20,"INCONSISTENT_DATA"),

    //状态码 401
    AUTH_INVALID_JWT(5,"AUTH_INVALID_JWT"),
    AUTH_JWT_EXPIRED(6,"AUTH_JWT_EXPIRED"),
    AUTH_INVALID_ACCOUNT(12, "AUTH_INVALID_ACCOUNT"),
    AUTH_ID_NOTEXIST(13,"AUTH_ID_NOTEXIST"),
    AUTH_USER_FORBIDDEN(14,"AUTH_USER_FORBIDDEN"),
    AUTH_NEED_LOGIN(15, "AUTH_NEED_LOGIN"),

    //状态码 403
    AUTH_NO_RIGHT(16, "AUTH_NO_RIGHT"),
    RESOURCE_ID_OUTSCOPE(17,"RESOURCE_ID_OUTSCOPE");


    private int errNo;
    private String message;
    private static final Map<Integer, ReturnNo> returnNoMap = new HashMap<Integer, ReturnNo>() {
        {
            for (ReturnNo enum1 : values()) {
                put( enum1.errNo, enum1);
            }
        }
    };

    ReturnNo(int code, String message){
        this.errNo = code;
        this.message = message;
    }

    public static ReturnNo getByCode(int code1) {
        ReturnNo[] all=ReturnNo.values();
        for (ReturnNo returnNo :all) {
            if (returnNo.errNo==code1) {
                return returnNo;
            }
        }
        return null;
    }
    public static ReturnNo getReturnNoByCode(int code){
        return returnNoMap.get(code);
    }
    public int getErrNo() {
        return errNo;
    }

    public String getMessage(){
        return message;
    }


    }
