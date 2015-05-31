package kvstore

import akka.actor.Actor
import scala.concurrent.Future
import akka.actor.Props
import akka.actor.ActorRef
import akka.actor.ActorLogging

object TimeoutHandler {
  case class TimeoutMessage(msg: Any, id: Long)
  case class Timedout(msg: Any, id: Long)
  
  def props: Props = Props(new TimeoutHandler)
}

class TimeoutHandler extends Actor {
  import TimeoutHandler._
  import context.dispatcher
  
  var timeoutMap = Map.empty[Long,(ActorRef,Any)]
  
  def receive: Receive = {

    case TimeoutMessage(msg: Any,id: Long) => {
      
      timeoutMap += (id -> (sender,msg))

      Future[Unit] {
        Thread.sleep(1000)
   
        val (tsnd, msg) = timeoutMap(id)
             
        tsnd ! Timedout(msg,id)
      }
    }
  }
}