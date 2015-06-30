package prototype

import akka.actor.{Props, ActorSystem, Actor}
import com.rabbitmq.client.{QueueingConsumer, ConnectionFactory}

import scala.concurrent.Future

import scala.concurrent.duration._

import scala.util.Random

case class NewJob(s: String)

case class NewStatus(id: String, status: String)

case class PollNewJob(q: String)

case class PollNewStatus(q: String)

case object Stop

object JobManager {

  val factory = new ConnectionFactory()
  factory.setHost("192.168.56.101")
  val connection = factory.newConnection()
  val channel = connection.createChannel()

  channel.queueDeclare("jm_newjob", false, false, false, null)
  channel.queueDeclare("w_newjob", false, false, false, null)
  channel.queueDeclare("jm_status", false, false, false, null)

  val newJobConsumer = new QueueingConsumer(channel)
  channel.basicConsume("jm_newjob", true, newJobConsumer)

  val newStatusConsumer = new QueueingConsumer(channel)
  channel.basicConsume("jm_status", true, newStatusConsumer)

  val system = ActorSystem("JobManager")
  val jm = system.actorOf(Props[JobManagerActor])
  val jp = system.actorOf(Props[JobPollingActor])
  val sp = system.actorOf(Props[StatusPollingActor])

  implicit val executor = system.dispatcher

  class JobPollingActor extends Actor {
    def receive = {
      case PollNewJob(q) =>
        val m = newJobConsumer.nextDelivery()
        jm ! NewJob(new String(m.getBody))
        self ! PollNewJob(q)
    }
  }

  class StatusPollingActor extends Actor {
    def receive = {
      case PollNewStatus(q) =>
        val m = newStatusConsumer.nextDelivery()
        jm ! NewStatus(m.getEnvelope.getDeliveryTag.toString, new String(m.getBody))
        self ! PollNewStatus(q)
    }
  }

  class JobManagerActor extends Actor {

    def receive = {
      case NewJob(m) =>
        System.out.println("received new job: " + m)
        channel.basicPublish("", "w_newjob", null, m.getBytes)
        if (m.equalsIgnoreCase("stop")) self ! Stop
      case NewStatus(m, s) => System.out.println("received new status: " + s)
      case Stop =>
        system.shutdown()
        System.exit(1)
    }

  }

  def main(args: Array[String]): Unit = {

    system.scheduler.scheduleOnce(0 seconds) {
      jp ! PollNewJob("jm_newjob")
      sp ! PollNewStatus("jm_status")
    }

  }

}

object Worker {

  val random = new Random()

  val factory = new ConnectionFactory()
  factory.setHost("192.168.56.101")
  val connection = factory.newConnection()
  val channel = connection.createChannel()

  channel.queueDeclare("w_newjob", false, false, false, null)

  val newJobConsumer = new QueueingConsumer(channel)
  channel.basicConsume("w_newjob", true, newJobConsumer)

  val system = ActorSystem("Worker")
  val w = system.actorOf(Props[WorkerActor])
  val p = system.actorOf(Props[PollingActor])

  implicit val executor = system.dispatcher

  class PollingActor extends Actor {
    def receive = {
      case PollNewJob(q) =>
        val m = newJobConsumer.nextDelivery()
        w ! NewJob(new String(m.getBody))
        self ! PollNewJob(q)
    }
  }

  class WorkerActor extends Actor {

    def receive = {
      case NewJob(m) =>
        Future {
          System.out.println("received new job: " + m)
          channel.basicPublish("", "jm_status", null, "running".getBytes)
          Thread.sleep((random.nextInt(9) + 1) * 1000)
          channel.basicPublish("", "jm_status", null, "finished".getBytes)
          if (m.equalsIgnoreCase("stop")) self ! Stop
        }
      case Stop =>
        system.shutdown()
        System.exit(1)
    }
  }

  def main(args: Array[String]): Unit = {

    system.scheduler.scheduleOnce(0 seconds) {
      p ! PollNewJob("w_newjob")
    }

  }

}

object Test {

  val factory = new ConnectionFactory()
  factory.setHost("192.168.56.101")
  val connection = factory.newConnection()
  val channel = connection.createChannel()

  channel.queueDeclare("jm_newjob", false, false, false, null)

  def main(args: Array[String]): Unit = {

    for (i <- 1 until 1000) {

      channel.basicPublish("", "jm_newjob", null, i.toString.getBytes)

    }

    channel.close()
    connection.close()

    //    sqs.sendMessage("jm_newjob", "stop")

  }

}