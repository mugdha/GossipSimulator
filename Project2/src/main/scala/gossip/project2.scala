package gossip

/*
 * project2.scala is the second project for COP5615 Distributed Operating Systems Principles 
 * at University of Florida. In this project, we use the akka actors framework in scala to 
 * implement a gossip algorithm for different topologies and calculate average over it 
 * using push-sum method
 * 
 * @authors - Mugdha Mugdha and Palak Shah
 * @date - 10/06/2014
 * 
 */
import akka.actor.ActorSystem
import akka.actor.Props

/*
 * project2 contains the main method for this project.
 *  
 */
object project2 {

  /*
   * Main Method
   * @param number of nodes in the topology
   * @param type of topology
   * @param type of algorithm
   * 
   */
  def main(args: Array[String]) {

    if (args.length > 2) {
      println("Gossip Simulator started ...\n")
      var numNod = args(0)
      var topology = args(1)
      var algo = args(2)
      var system = ActorSystem("GossipSimulator")

      /*
       * If topology is a 2d-grid round up the number of nodes to the nearest square
       * smaller than given number
       */
      if (topology.contains("2D")) {
        var sqrt: Int = Math.sqrt(numNod.toInt).toInt
        numNod = Math.pow(sqrt, 2).toInt.toString
      }

      /*
       * The boss actor only does one thing - terminate the topology 
       * For Gossip, it does this when all actors receive message atleast once
       * For push-sum, it does this when one of the actor converges to the correct value
       * 
       */
      val boss = system.actorOf(Props(new bossActor(numNod.toInt, system)), name = "boss")
      boss ! "start"

      /*
       * Initialize all the nodes and build topology by creating their neighbour list
       */
      println("Initializing " + numNod  + " nodes ...\n")
      for (j <- 0 to numNod.toInt - 1) {
        var node = system.actorOf(Props(new nodeActor(numNod.toInt, topology, algo, j, boss)), name = j.toString)
        node ! "start"
      }
      println("Building " + topology + " topology ...\n")

      /*
       * Select node to which we will send first message
       * In this case it is the middle node
       */
      var startNode = (numNod.toInt / 2)
      var firstNode = system.actorFor("/user/" + startNode)
      println("first node :: " + firstNode.path + '\n')

      /*
       * Start the algorithm by sending appropriate message to the node selected above
       */
      if (algo.equalsIgnoreCase("gossip")) {
        println("Starting Gossip Algo ... \n")
        firstNode ! loopGossip("This is a message")
      } else if (algo.equalsIgnoreCase("push-sum")) {
        println("Starting Push-Sum Algo ... \n")
        firstNode ! loopPS(0.0, 0.0)
      }

    } else {
      //Handle case where user enters wrong value
      println("Usage : project2 <#nodes> <topology> <algorithm>")
    }
  }
}
