package kvstore

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import scala.concurrent.Future

object PersistSender {
  case class SendPersist(key: String, valueOption: Option[String], seq: Long)
  case class PersistSent(key: String, seq: Long)

  def props(persistActor: ActorRef): Props = Props(new PersistSender(persistActor))
}
class PersistSender(val persistActor: ActorRef) extends Actor {
  import PersistSender._
  import Persistence._
  import context.dispatcher

  var persistMap = Map.empty[Long, ActorRef]

  def receive: Receive = {
    case SendPersist(key, valueOption, seq) => {
      persistMap += (seq -> sender)
      Future[Unit] {
        while (persistMap.keySet.contains(seq)) {
          persistActor ! Persist(key, valueOption, seq)
          Thread.sleep(100)
        }
      }

    }
    case Persisted(key, seq) => {
      val psnd = persistMap(seq)
      persistMap -= seq
      psnd ! PersistSent(key, seq)
    }
    case _ =>
  }
}