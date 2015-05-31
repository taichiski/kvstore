package kvstore

import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorRef
import scala.concurrent.duration._
import akka.actor.Cancellable
import akka.actor.ActorLogging
import akka.event.LoggingReceive

object SnapshotSender {
  case class SendSnapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotSent(key: String, seq: Long)

  def snapshotSenderProps(replica: ActorRef): Props = Props(new SnapshotSender(replica))
}

class SnapshotSender(val replica: ActorRef) extends Actor with ActorLogging {
  import SnapshotSender._
  import Replicator._
  import context.dispatcher

  var snapshotMap = Map.empty[Long,(ActorRef,Cancellable)]
  
  def receive: Receive = LoggingReceive {
    case SendSnapshot(key, valueOption, seq) => { 
      val scheduler = context.system.scheduler.schedule(Duration(0,"milliseconds"), Duration(100,"milliseconds")) {
        replica ! Snapshot(key,valueOption,seq)  
      }
      snapshotMap += (seq -> (sender,scheduler))
    }
    case SnapshotAck(key,seq) => {
      val (ssnd,scheduler) = snapshotMap(seq)
      snapshotMap -= seq
      scheduler.cancel()
      ssnd ! SnapshotSent(key,seq)
    }
    case _ =>
  }
}