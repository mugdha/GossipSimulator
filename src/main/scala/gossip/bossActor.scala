package gossip

/*
 * bossActor is not a part of the topology. It's only function is to terminate the actorSystem
 * when the algorithm converges 
 * For Gossip, convergence is defined as all actors receiving message at least once
 * For push-sum, convergence is defined as one of the actor converging to the correct value
 * 
 */
import akka.actor.ActorSystem
import akka.actor.Actor
import scala.sys.Prop
import akka.actor.Props
import akka.actor.ActorRef
import akka.actor.PoisonPill
import scala.collection.mutable.ListBuffer
import scala.util.Random

case class hi(rounds: Int, nodeNo: Int) //declaration for a message called hi

/*
 * bossActor class
 * @param total nodes in the system
 * @param actor system for this actor
 * 
 */
class bossActor(totalNode: Int, mysys: ActorSystem) extends Actor {
  var nodesReceivedOnce: Int = 0
  var roundCount: Int = 0
  var starttime: Long = System.currentTimeMillis()

  
  def receive = {

    /*
     * This message will come from a nodeActor when it receives it's first message 
     * On receiving this message, this actor will increment a counter
     * When the counter = total number of nodes, it will terminate the system
     * @param number of rounds taken to converge
     * @param identifier of node that sends this message
     */
    case hi(rounds: Int, nodeNo: Int) =>
      if (nodesReceivedOnce == 0) {
        starttime = System.currentTimeMillis()
      }
      //println("Node " + nodeNo + " got the message")
      nodesReceivedOnce += 1
      roundCount = rounds
      if (nodesReceivedOnce >= totalNode) {
        println("Round counter::" + roundCount)
        terminate
      }

    /*
     * This message will come from a nodeActor when it converges in push-sum algorithm 
     * On receiving this message, this actor will terminate the system
     */
    case "byePushSum" =>
      terminate

  }

  /*
   * Print the time taken for execution and terminate the system
   */
  def terminate = {
    println("========================================================================")
    println("Time taken for execution :: " + (System.currentTimeMillis - starttime).toString + " ms")
    println("shutting down ... ")
    println("========================================================================")
    mysys.shutdown
  }

}
