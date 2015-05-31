package kvstore

import akka.actor.Actor
import scala.concurrent.Future
import akka.actor.Props
import akka.actor.ActorRef
import akka.actor.ActorLogging
import scala.concurrent.duration._

object TimeoutHandler {
  case class TimeoutMessage(msg: Any, id: Long)
  case class Timedout(msg: Any, id: Long)
  
  def props: Props = Props(new TimeoutHandler)
}

class TimeoutHandler extends Actor {
  import TimeoutHandler._
  import context.dispatcher
  
  def receive: Receive = {

    case TimeoutMessage(msg: Any,id: Long) => {
      val tsnd = sender
      context.system.scheduler.scheduleOnce(Duration(1,"second")) {
        tsnd ! Timedout(msg,id)
      }
    }
  }
}