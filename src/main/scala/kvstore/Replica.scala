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
import akka.actor.ActorLogging
import akka.event.LoggingReceive

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

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher
  import PersistSender._
  import TimeoutHandler._
  
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
  
  // Timeout
  val timeoutActor = context.actorOf(TimeoutHandler.props,"timeout-actor")
  var timeoutMap = Map.empty[Long,ActorRef]
  
  // Replication
  var _replicatorId = 0L
  def nextReplicatorId: Long = {
    val ret = _replicatorId
    _replicatorId += 1
    ret
  }

  var _replicateId = 0L
  def nextReplicateId: Long = {
    val ret = _replicateId
    _replicateId += 1
    ret
  }

  override def preStart = {
    arbiter ! Join
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  val leader: Receive = LoggingReceive {
    case msg @ Insert(key, value, id) => {
      kv += (key -> value)
      
      for(replicator <- replicators) {
        replicator ! Replicate(key,Some(value),nextReplicateId)
      }
      persistMap += (id -> (sender, msg))
      
      val persistMsg = SendPersist(key, Some(value), id)
      persistSender ! persistMsg
      
      timeoutMap += (id -> sender)
      timeoutActor ! TimeoutMessage(persistMsg,id)
    }
    case msg @ Remove(key, id) => {
      kv -= key
      
      for(replicator <- replicators) {
        replicator ! Replicate(key,None,nextReplicateId)
      }
      
      persistMap += (id -> (sender, msg))
      val persistMsg = SendPersist(key,None,id)
      
      persistSender ! persistMsg
      
      timeoutMap += (id -> sender)
      timeoutActor ! TimeoutMessage(persistMsg,id)
    }
    
    case PersistSent(key, id) => {
      
      val opt = persistMap.get(id)
      opt match {
        case Some((psnd,msg)) => {
          persistMap -= id
          val topt = timeoutMap.get(id)
          topt match {
            case Some(tsnd) => timeoutMap -= id
            case None =>
          }
          msg match {
            case Insert(_,_,_) => psnd ! OperationAck(id)
            case Remove(_,_) => psnd ! OperationAck(id)
            case _ =>
          }
        }
        case None =>
      }
    }
    
    case Timedout(msg,id) => {
      
      val tsndOpt = timeoutMap.get(id)
      tsndOpt match {
        case Some(tsnd) => {
          msg match {
            case SendPersist(_,_,_) => {
              val pOpt = persistMap.get(id)
              pOpt match {
                case Some((psnd,msg)) => persistMap -= id
                case None =>
              }
              tsnd ! OperationFailed(id)
            }
            case None =>
          }
          
        }
        case None =>
      }
    }
    case Replicas(replicas) => {
      val secondaryReplicas = replicas.filterNot(_ == self)
      val newSecondaryReplicas = secondaryReplicas.filterNot(secondaries.keySet.contains(_))
      val removeSecondaryReplicas = secondaries.keySet.filterNot(secondaryReplicas.contains(_))
      
      for(replica <- newSecondaryReplicas) {
        val name = s"replicator-$nextReplicatorId"
        val replicator = context.actorOf(Replicator.props(replica),name)
        secondaries += (replica -> replicator)
        replicators += replicator
        
        for((key, value) <- kv) {
          replicator ! Replicate(key,Some(value),nextReplicateId)
        }
      }
      
      for(replica <- removeSecondaryReplicas) {
        context.stop(replica)
        val replicatorOpt = secondaries.get(replica)
        replicatorOpt match {
          case Some(replicator) => {
            context.stop(replicator)
            secondaries -= replica
            replicators -= replicator
          }
          case None =>
        }
      }
    }
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case _ =>
  }

  val replica: Receive = LoggingReceive {
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

