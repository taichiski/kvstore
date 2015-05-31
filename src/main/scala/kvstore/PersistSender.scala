package kvstore

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import scala.concurrent.Future
import akka.actor.Cancellable

import scala.concurrent.duration._
import scala.language.postfixOps

object PersistSender {
  case class SendPersist(key: String, valueOption: Option[String], seq: Long)
  case class PersistSent(key: String, seq: Long)

  def props(persistActor: ActorRef): Props = Props(new PersistSender(persistActor))
}
class PersistSender(val persistActor: ActorRef) extends Actor {
  import PersistSender._
  import Persistence._
  import context.dispatcher

  var persistMap = Map.empty[Long, (ActorRef, Cancellable)]

  def receive: Receive = {
    case SendPersist(key, valueOption, seq) => {
      val persistMsg = Persist(key,valueOption,seq)
      val scheduler = context.system.scheduler.schedule(Duration(0,"milliseconds"),Duration(100,"milliseconds")) {
        persistActor ! persistMsg
      }
      persistMap += (seq -> (sender,scheduler))
    }
    case Persisted(key, seq) => {
      val (psnd,cancellable) = persistMap(seq)
      persistMap -= seq
      cancellable.cancel()
      psnd ! PersistSent(key, seq)
    }
    case _ =>
  }
}