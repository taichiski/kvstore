package kvstore

import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.Future
import akka.actor.Props

object ReplicateSender {
  case class SendReplicate(key: String, valueOption: Option[String], id: Long)  
  case class ReplicateSent(key: String, id: Long)
  
  def props(replica: ActorRef): Props = Props(new ReplicateSender(replica))
}

class ReplicateSender(val replica: ActorRef) extends Actor {
  import Replicator._
  import ReplicateSender._
  import context.dispatcher
  
  var senderMap = Map.empty[Long,ActorRef]
  def receive: Receive = {
    case SendReplicate(key,valueOption,seq) => {
        senderMap += (seq -> sender)
        Future[Unit] {
          while(senderMap.keySet.contains(seq)) {
            replica ! Snapshot(key,valueOption,seq)
            Thread.sleep(100)
          }
        }
    }
    case SnapshotAck(key,seq) => {
      val rsnd = senderMap(seq)
      senderMap -= seq
      rsnd ! ReplicateSent(key,seq)
    }
    case _ =>
  }
}