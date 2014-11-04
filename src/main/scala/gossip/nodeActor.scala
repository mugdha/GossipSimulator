package gossip

/*
 * nodeActor is one of the nodes in the topology. It participates in gossip and push-sum algos
 * The logic for building the topology and executing algorithms is written here
 * 
 */
import akka.actor.Actor
import scala.collection.mutable.ArrayBuffer
import akka.actor.ActorRef
import akka.actor.ActorSystem
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import util.Random

case class loopGossip(message: String) //declaration for a message called loopGossip
case class loopPS(s: Double, w: Double) //declaration for a message called loopPS

class nodeActor(totalNode: Int, topology: String, algo: String, nodeNo: Int, boss: ActorRef) extends Actor {

  var neighbour = new ListBuffer[Int];
  var msgCount: Int = 0
  var msg: String = null
  var roundCounter: Int = 0
  var s: Double = nodeNo.toDouble
  var w = 1.0
  var ratio = 0.0;
  var ratio_old = 999.0; var dratio = 999.0;
  var flag: Boolean = false
  var epsilonCount = 0 // termination the execution through push sum

  val roundScheduler = context.system.scheduler.schedule(0 seconds, 1 seconds, self, "startRound") //.scheduler.schedule(0 seconds, 5 minutes)(startRound)
  
  def receive = {
	/*
	 * Start the node and build its neighbour list according to the topology
	 */
    case "start" =>
      buildNeighbourList

    /*
     * Start each round and use a counter to keep track of the number of rounds completed
     */
    case "startRound" =>
      roundCounter += 1
      if (flag == true) {
        SendMessageToRandomNeighbour
      }

    /*
     * When another nodeActor sends a message corresponding to gossip topology,
     * we go into this class. loopGossip notifies the boss when an node recieves it's first message
     * When a node receives it's 10th message, it stops transmitting
     * We use a boolean flag to decide when a node should transmit
     * 
     * @param the message to be propogated through gossip
     * 
     */
    case loopGossip(message: String) =>
      if (msgCount == 0) {
        flag = true
        msg = message
        //println(self.path + " in loopGosip " + message + " flag = " + flag.toString)
        msgCount += 1
        boss ! hi(roundCounter,nodeNo)
      } else if (msgCount == 9) {
        flag = false
        //println("Node " +nodeNo+ " got the message 10 times. It is terminating")
      } else {
        msg = message
        //println(self.path + " in loopGosip " + message + " flag = " + flag.toString)
        msgCount += 1
      }

    /*
     * When another nodeActor sends a message corresponding to gossip topology,
     * we go into this class. loopPS sends half of the s and w of current node to a random neighbour
     * It terminates when a node reports the same ratio (difference < 10^10) 3 times
     */
    case loopPS(sReceived: Double, wReceived: Double) =>
      //println("in LoopPS")
      if (epsilonCount == 0) {
        flag = true
        Calculate(sReceived, wReceived)
      } else if (epsilonCount == 3) {
        flag = false
        println("Node " + self.path + " converged")
        println("round counter::" + roundCounter)
        println("avg value" + ratio)
        boss ! "byePushSum" // killing the system
      } else if (epsilonCount > 3) {
        println("Terminated actor is getting messages")
      } else {
        flag = true
        Calculate(sReceived, wReceived)
      }

  }

  /*
   * Select a random neighbour from neighbourlist and send it a message
   */
  def SendMessageToRandomNeighbour = {
    //roundCounter += 1
    var randIndex = Random.nextInt(neighbour.length)
    var randNeighbour = context.actorSelection("akka://GossipSimulator/user/" + neighbour.apply(randIndex).toString)
    algo.toLowerCase() match {
      case "gossip" =>
        randNeighbour ! loopGossip(msg)

      case "push-sum" =>
        randNeighbour ! loopPS(s/2, w/2)
        s /= 2
        w /= 2
    }
  }

  /*
   * calculate values of s, w and ratio for push-sum algorithm
   */
  def Calculate(sReceived: Double, wReceived: Double) = {
    s = (s + sReceived)// / 2
    w = (w + wReceived)// / 2
    ratio_old = ratio
    ratio = s / w
    dratio = (ratio - ratio_old).abs
    if (dratio < 0.0000000001) epsilonCount += 1
  }

  /*
   * Build neighbour list according to topology
   */
  def buildNeighbourList = {
    topology.toLowerCase() match {

      case "line" =>
        if (nodeNo - 1 >= 0)
          neighbour += nodeNo - 1 //left neighbour
        if (nodeNo + 1 < totalNode)
          neighbour += nodeNo + 1 //right neighbour

      case "2d" =>
        var sqrt: Int = Math.sqrt(totalNode).toInt
        if (nodeNo % sqrt != 0)
          neighbour += nodeNo - 1 //left neighbour
        if (nodeNo % sqrt != sqrt - 1)
          neighbour += nodeNo + 1 //right neighbour
        if ((nodeNo / sqrt) > 0)
          neighbour += nodeNo - sqrt //above neighbour
        if ((nodeNo / sqrt) < sqrt - 1)
          neighbour += nodeNo + sqrt //below neighbour

      case "imp2d" =>
        var sqrt: Int = Math.sqrt(totalNode).toInt
        val actualN = math.pow(sqrt, 2).toInt
        var rndInt = Int.MaxValue
        if (nodeNo % sqrt != 0)
          neighbour += nodeNo - 1 //left neighbour
        if (nodeNo % sqrt != sqrt - 1)
          neighbour += nodeNo + 1 //right neighbour
        if ((nodeNo / sqrt) > 0)
          neighbour += nodeNo - sqrt //above neighbour
        if ((nodeNo / sqrt) < sqrt - 1)
          neighbour += nodeNo + sqrt //below neighbour
        //add random neighbour
        while (rndInt > actualN || rndInt == 0) {
          rndInt = (Random.nextInt % actualN).abs
        }
        neighbour += rndInt

      case "full" =>
        for (i <- 0 to totalNode - 1) {
          if (i != nodeNo)
            neighbour += i
        }

    }
  }
}
