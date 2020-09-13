package annotation;

import java.lang.annotation.*;

/**
 * Author:BYDylan
 * Date:2020/9/9
 * Description: 自定义注解,用于描述字段所属的 family与qualifier。也就是hbase的列与列簇
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@Inherited
public @interface HBaseColumn {
    String family() default "f";

    String qualifier() default "";

    String desc() default "";

    EnumStoreType columnType() default EnumStoreType.EST_STRING;

    boolean timestamp() default false;
}

