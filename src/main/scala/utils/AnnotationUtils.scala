package utils

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

/**
 * Author:BYDylan
 * Date:2020/9/9
 * Description:
 */
class AnnotationUtils {
  // 获取指定类型的注解信息，通过 Annotation.tree.tpe 获取注解的 Type 类型，以此进行筛选
  def getClassAnnotation[T: TypeTag, U: TypeTag]: String = {
    var resultResult: String = "";
    val option: Option[universe.Annotation] = symbolOf[T].annotations.find(_.tree.tpe =:= typeOf[U])
    option.map(_.tree).map { memberAnnotationTree =>
      val Apply(_, Literal(Constant(tableName: String)) :: Nil) = memberAnnotationTree;
      resultResult = tableName;
    }
    return resultResult;
  }

  // 通过字段名称获取指定类型的注解信息，注意查找字段名称时添加空格
  def getMemberAnnotation[T: TypeTag, U: TypeTag](memberName: String) = {
    val maybeAnnotation: Option[universe.Annotation] = typeOf[T].decl(TermName(s"$memberName ")).annotations.find(_.tree.tpe =:= typeOf[U])
    maybeAnnotation.map(_.tree).foreach { memberAnnotationTree =>
      val Apply(_, Literal(Constant(qualifier: String)) :: Literal(Constant(family: String)) :: Nil) = memberAnnotationTree
      println(qualifier);
      println(family);
    }
  }
}
