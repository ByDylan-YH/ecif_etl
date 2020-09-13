package annotation;

import java.lang.annotation.*;

/**
 * Author:BYDylan
 * Date:2020/9/9
 * Description: 自定义注解,用于获取table
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Inherited
public @interface HBaseTable {
    String tableName() default "";
}

