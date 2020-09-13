package entity

import server.TaskServer

import scala.collection.mutable

/**
 * Author:BYDylan
 * Date:2020/8/29
 * Description: 任务池
 */
class TaskPoolEntity {
  var taskMap: mutable.HashMap[String, TaskServer] = new mutable.HashMap[String, TaskServer]();

  def addTask2Pool(taskName: String, task: TaskServer): Unit = {
    taskMap.+=(taskName -> task);
  }
}
