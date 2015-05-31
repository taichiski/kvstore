package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher
  import PersistSender._
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  // Secondary
  var expectedSeq = 0L

  // Persistence
  val persistActor = context.actorOf(persistenceProps, "persist-actor")
  val persistSender = context.actorOf(PersistSender.props(persistActor), "persist-sender")
  var persistMap = Map.empty[Long, (ActorRef, Any)]

  override def preStart = {
    arbiter ! Join
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case msg @ Insert(key, value, id) => {
      kv += (key -> value)
      persistMap += (id -> (sender, msg))
      persistSender ! SendPersist(key, Some(value), id)
    }
    case msg @ Remove(key, id) => {
      kv -= key
      persistMap += (id -> (sender, msg))
      persistSender ! SendPersist(key, None, id)
    }
    case PersistSent(key, id) => {
      val (psnd, msg) = persistMap(id)
      persistMap -= id
      msg match {
        case Insert(key, value, id) => {
          psnd ! OperationAck(id)
        }
        case Remove(key, id) => {
          psnd ! OperationAck(id)
        }
        case _ =>
      }
    }
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case _ =>
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case msg @ Snapshot(key, valueOption, seq) => {
      if (seq == expectedSeq) {
        valueOption match {
          case Some(value) => kv += (key -> value)
          case None        => kv -= key
        }

        persistMap += (seq -> (sender, msg))
        persistSender ! SendPersist(key, valueOption, seq)

        expectedSeq += 1

      } else if (seq < expectedSeq) {
        sender ! SnapshotAck(key, seq)
      }

    }
    case PersistSent(key, seq) => {
      val (ssnd, msg) = persistMap(seq)
      persistMap -= seq
      msg match {
        case Snapshot(key,valueOption,seq) => {
          ssnd ! SnapshotAck(key,seq)
        }
        case _ =>
      }
    }
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case _ =>
  }

}

