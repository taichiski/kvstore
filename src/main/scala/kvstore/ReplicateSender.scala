package kvstore

import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.Future
import akka.actor.Props
import akka.actor.ActorLogging
import akka.event.LoggingReceive
import akka.actor.Cancellable

import scala.concurrent.duration._

object ReplicateSender {
  case class SendReplicate(key: String, valueOption: Option[String], id: Long)
  case class ReplicateSent(key: String, id: Long)

  def props(replica: ActorRef): Props = Props(new ReplicateSender(replica))
}

class ReplicateSender(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._
  import ReplicateSender._
  import context.dispatcher

  var senderMap = Map.empty[Long, (ActorRef, Cancellable)]

  def receive: Receive = LoggingReceive {
    case SendReplicate(key, valueOption, seq) => {
      val scheduler = context.system.scheduler.schedule(Duration(0, "milliseconds"), Duration(100, "milliseconds")) {
        replica ! Snapshot(key, valueOption, seq)
      }
      senderMap += (seq -> (sender, scheduler))
    }
    case SnapshotAck(key, seq) => {
      val (rsnd,rscheduler) = senderMap(seq)
      senderMap -= seq
      rscheduler.cancel()
      rsnd ! ReplicateSent(key, seq)
    }
    case _ =>
  }
}